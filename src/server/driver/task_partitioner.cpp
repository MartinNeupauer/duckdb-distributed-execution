#include "server/driver/task_partitioner.hpp"

#include "server/driver/distributed_executor.hpp"
#include "server/driver/partition_sql_generator.hpp"
#include "server/driver/query_plan_analyzer.hpp"

#include <algorithm>
#include <unordered_set>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

vector<DistributedPipelineTask> TaskPartitioner::ExtractIcebergCountTasks(const string &table_name,
                                                                         idx_t num_workers) {
	auto metadata = conn.Query("SELECT file_path, manifest_content, status FROM iceberg_metadata(" +
	                           Value(table_name).ToSQLString() + ")");
	if (metadata->HasError()) {
		throw IOException("Failed to list Iceberg data files for %s: %s", table_name, metadata->GetError());
	}

	vector<string> files;
	unordered_set<string> seen;
	for (auto chunk = metadata->Fetch(); chunk && chunk->size() > 0; chunk = metadata->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); ++row) {
			const auto content = chunk->GetValue(1, row).ToString();
			const auto status = chunk->GetValue(2, row).ToString();
			if (StringUtil::CIEquals(status, "DELETED")) {
				continue;
			}
			if (!StringUtil::CIEquals(content, "DATA")) {
				throw NotImplementedException("Iceberg file sharding does not support live delete files in %s", table_name);
			}
			auto path = chunk->GetValue(0, row).ToString();
			if (!seen.insert(path).second) {
				throw IOException("Iceberg metadata lists data file %s more than once", path);
			}
			files.emplace_back(std::move(path));
		}
	}
	if (files.empty()) {
		return {};
	}
	std::sort(files.begin(), files.end());
	const idx_t num_tasks = std::min(num_workers, files.size());
	vector<vector<string>> assignments(num_tasks);
	for (idx_t index = 0; index < files.size(); ++index) {
		assignments[index % num_tasks].emplace_back(std::move(files[index]));
	}

	vector<DistributedPipelineTask> tasks;
	tasks.reserve(num_tasks);
	for (idx_t task_idx = 0; task_idx < num_tasks; ++task_idx) {
		DistributedPipelineTask task;
		vector<string> paths;
		paths.reserve(assignments[task_idx].size());
		for (const auto &path : assignments[task_idx]) {
			paths.emplace_back(Value(path).ToSQLString());
			task.load_httpfs = task.load_httpfs || StringUtil::StartsWith(path, "s3://");
		}
		task.task_id = task_idx;
		task.total_tasks = num_tasks;
		task.partition_strategy = PartitionStrategy::ICEBERG_FILES;
		task.task_sql = "SELECT count(*) AS __c0 FROM read_parquet([" + StringUtil::Join(paths, ", ") + "])";
		tasks.emplace_back(std::move(task));
	}
	return tasks;
}

TaskPartitioner::TaskPartitioner(Connection &conn_p, QueryPlanAnalyzer &analyzer_p)
    : conn(conn_p), analyzer(analyzer_p) {
}

vector<DistributedPipelineTask> TaskPartitioner::CreateSingleTask(const string &base_sql) {
	vector<DistributedPipelineTask> tasks;
	DistributedPipelineTask task;
	task.task_id = 0;
	task.total_tasks = 1;
	task.task_sql = base_sql;
	task.row_group_start = 0;
	task.row_group_end = 0;
	tasks.emplace_back(std::move(task));
	return tasks;
}

vector<DistributedPipelineTask> TaskPartitioner::ExtractPipelineTasks(LogicalOperator &logical_plan,
                                                                      const string &base_sql, idx_t num_workers) {
	if (num_workers == 0) {
		return {};
	}

	// Only partition direct DuckDB table scans; delegate other supported queries intact.
	auto statements = conn.ExtractStatements(base_sql);
	if (statements.size() != 1 || statements[0]->type != StatementType::SELECT_STATEMENT) {
		return {};
	}
	auto &statement = statements[0]->Cast<SelectStatement>();
	if (statement.node->type != QueryNodeType::SELECT_NODE) {
		return CreateSingleTask(base_sql);
	}
	auto &select = statement.node->Cast<SelectNode>();
	if (!select.from_table || select.from_table->type != TableReferenceType::BASE_TABLE ||
	    !select.cte_map.map.empty() || !select.modifiers.empty() || select.sample || select.from_table->sample ||
	    !select.from_table->column_name_alias.empty()) {
		return CreateSingleTask(base_sql);
	}
	auto *op = &logical_plan;
	while (op->children.size() == 1) {
		op = op->children[0].get();
	}
	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return CreateSingleTask(base_sql);
	}
	auto table = op->Cast<LogicalGet>().GetTable();
	auto &table_ref = select.from_table->Cast<BaseTableRef>();
	if (!table || !table->IsDuckTable() || !StringUtil::CIEquals(table_ref.table_name, table->name)) {
		return CreateSingleTask(base_sql);
	}
	// Resolve the source on the driver so workers do not depend on their default catalog.
	table_ref.catalog_name = table->catalog.GetName();
	table_ref.schema_name = table->schema.name;
	const string task_sql = statement.ToString();

	// Extract row group information for DuckDB-aligned partitioning
	// If reliable rowid bounds are unavailable, delegate instead of using modulo-based partitioning.
	auto row_group_info = analyzer.ExtractRowGroupInfo(logical_plan);
	if (!row_group_info.valid || row_group_info.total_row_groups == 0) {
		return CreateSingleTask(task_sql);
	}
	// Give each worker one contiguous run of whole row groups. Rowid pruning is row-group granular, so splitting a
	// row group makes every task read all of it; with static assignment, extra tasks per worker only break locality.
	// Tables with fewer row groups than workers leave the remaining workers idle.
	const idx_t num_tasks = std::min(num_workers, row_group_info.total_row_groups);
	const idx_t groups_per_task = row_group_info.total_row_groups / num_tasks;
	const idx_t remainder = row_group_info.total_row_groups % num_tasks;
	const string rowid =
	    ColumnRefExpression("rowid", table_ref.alias.empty() ? table_ref.table_name : table_ref.alias).ToString();
	vector<DistributedPipelineTask> tasks;
	tasks.reserve(num_tasks);
	for (idx_t task_idx = 0; task_idx < num_tasks; ++task_idx) {
		// Calculate which row groups this task processes; the first `remainder` tasks take one extra.
		const idx_t rg_start = task_idx * groups_per_task + std::min(task_idx, remainder);
		const idx_t rg_end = rg_start + groups_per_task + (task_idx < remainder ? 1 : 0);
		// Use actual storage starts instead of multiplying by an approximate row-group size.
		const idx_t row_start = row_group_info.row_group_starts[rg_start];
		const idx_t row_end = rg_end == row_group_info.total_row_groups ? row_group_info.rowid_end
		                                                                : row_group_info.row_group_starts[rg_end];
		DistributedPipelineTask task;
		task.task_id = task_idx;
		task.total_tasks = num_tasks;
		task.partition_strategy = num_tasks == 1 ? PartitionStrategy::NONE : PartitionStrategy::ROW_GROUP_ALIGNED;
		task.row_group_start = rg_start;
		task.row_group_end = rg_end - 1;
		// Create SQL with row group-aligned half-open rowid filter.
		task.task_sql = PartitionSQLGenerator::InjectWhereClause(
		    task_sql, StringUtil::Format("%s >= %llu AND %s < %llu", rowid, row_start, rowid, row_end));
		tasks.emplace_back(std::move(task));
	}
	return tasks;
}

} // namespace duckdb
