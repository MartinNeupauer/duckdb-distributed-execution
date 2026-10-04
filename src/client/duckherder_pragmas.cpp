#include "duckherder_pragmas.hpp"

#include "client/execution/distributed_client.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckherder_catalog.hpp"

namespace duckdb {

namespace {

struct DuckherderQueryData : public TableFunctionData {
	explicit DuckherderQueryData(shared_ptr<QueryResult> result_p) : result(std::move(result_p)) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DuckherderQueryData>(result);
	}
	bool Equals(const FunctionData &other) const override {
		return this == &other;
	}
	shared_ptr<QueryResult> result;
};

struct DuckherderQueryState : public GlobalTableFunctionState {
	explicit DuckherderQueryState(shared_ptr<QueryResult> result_p) : result(std::move(result_p)) {
	}
	shared_ptr<QueryResult> result;
};

unique_ptr<FunctionData> BindDuckherderQuery(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("duckherder_query requires a SQL string");
	}
	auto sql = input.inputs[0].ToString();
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1) {
		throw ParserException("duckherder_query expects exactly one SQL statement");
	}
	auto &db_manager = DatabaseManager::Get(context);
	auto dh_db = db_manager.GetDatabase(context, "dh");
	if (!dh_db) {
		throw CatalogException("Duckherder database 'dh' not attached");
	}
	auto &catalog = dh_db->GetCatalog();
	if (catalog.GetCatalogType() != "duckherder") {
		throw CatalogException("Database 'dh' is not a duckherder database");
	}
	auto &client = catalog.Cast<DuckherderCatalog>().GetClient(context);
	auto result = parser.statements[0]->type == StatementType::SELECT_STATEMENT
	                  ? client.ScanTable(sql)
	                  : client.ExecuteStatement(sql, parser.statements[0]->type, catalog.GetName());
	if (result->HasError()) {
		result->ThrowError();
	}
	return_types = result->types;
	names = result->names;
	return make_uniq<DuckherderQueryData>(shared_ptr<QueryResult>(std::move(result)));
}

unique_ptr<GlobalTableFunctionState> InitDuckherderQuery(ClientContext &context, TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<DuckherderQueryData>();
	return make_uniq<DuckherderQueryState>(data.result);
}

void DuckherderQuery(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<DuckherderQueryState>();
	auto chunk = state.result->Fetch();
	if (chunk) {
		output.Move(*chunk);
	}
}

} // namespace

/*static*/ PragmaFunction DuckherderPragmas::GetUnregisterRemoteTableFunction() {
	return PragmaFunction::PragmaCall("duckherder_unregister_remote_table", UnregisterRemoteTable,
	                                  {LogicalType {LogicalTypeId::VARCHAR}});
}

/*static*/ PragmaFunction DuckherderPragmas::GetQueryFunction() {
	return PragmaFunction::PragmaCall("duckherder_query", Query, {LogicalType {LogicalTypeId::VARCHAR}});
}

/*static*/ TableFunction DuckherderPragmas::GetQueryTableFunction() {
	return TableFunction("duckherder_query", {LogicalType {LogicalTypeId::VARCHAR}}, DuckherderQuery,
	                     BindDuckherderQuery, InitDuckherderQuery);
}

/*static*/ ScalarFunction DuckherderPragmas::GetLoadExtensionFunction() {
	return ScalarFunction("duckherder_load_extension", {LogicalType {LogicalTypeId::VARCHAR}},
	                      LogicalType {LogicalTypeId::BOOLEAN}, LoadExtension);
}

/*static*/ void DuckherderPragmas::UnregisterRemoteTable(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();

	// Get the duckherder catalog - assuming it's attached as "dh".
	auto &db_manager = DatabaseManager::Get(context);
	auto dh_db = db_manager.GetDatabase(context, "dh");
	if (!dh_db) {
		throw CatalogException("Duckherder database 'dh' not attached");
	}

	auto &catalog = dh_db->GetCatalog();
	if (catalog.GetCatalogType() != "duckherder") {
		throw CatalogException("Database 'dh' is not a duckherder database");
	}

	auto dh_catalog_ptr = dynamic_cast<DuckherderCatalog *>(&catalog);
	if (!dh_catalog_ptr) {
		throw CatalogException("Failed to cast catalog to DuckherderCatalog");
	}
	dh_catalog_ptr->UnregisterRemoteTable(table_name);
}

/*static*/ void DuckherderPragmas::Query(ClientContext &context, const FunctionParameters &parameters) {
	auto sql = parameters.values[0].ToString();
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1) {
		throw ParserException("duckherder_query expects exactly one SQL statement");
	}
	auto statement_type = parser.statements[0]->type;
	auto &db_manager = DatabaseManager::Get(context);
	auto dh_db = db_manager.GetDatabase(context, "dh");
	if (!dh_db) {
		throw CatalogException("Duckherder database 'dh' not attached");
	}
	auto &catalog = dh_db->GetCatalog();
	if (catalog.GetCatalogType() != "duckherder") {
		throw CatalogException("Database 'dh' is not a duckherder database");
	}
	auto &client = dynamic_cast<DuckherderCatalog &>(catalog).GetClient(context);
	auto result = client.ExecuteStatement(sql, statement_type, catalog.GetName());
	if (result->HasError()) {
		throw Exception(ExceptionType::EXECUTOR, StringUtil::Format("Server failed to execute statement %s: %s",
																	sql, result->GetError()));
	}
}

/*static*/ void DuckherderPragmas::LoadExtension(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &extension_name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(extension_name_vector, result, args.size(), [&](string_t extension_name) {
		auto extension_name_str = extension_name.GetString();
		auto &context = state.GetContext();

		// Load extension on server side.
		//
		// Get the duckherder catalog - assuming it's attached as "dh".
		auto &db_manager = DatabaseManager::Get(context);
		auto dh_db = db_manager.GetDatabase(context, "dh");
		if (!dh_db) {
			throw CatalogException("Duckherder database 'dh' not attached");
		}

		auto &catalog = dh_db->GetCatalog();
		if (catalog.GetCatalogType() != "duckherder") {
			throw CatalogException("Database 'dh' is not a duckherder database");
		}

		auto dh_catalog_ptr = dynamic_cast<DuckherderCatalog *>(&catalog);
		if (!dh_catalog_ptr) {
			throw CatalogException("Failed to cast catalog to DuckherderCatalog");
		}

		auto load_result = dh_catalog_ptr->GetClient(context).LoadExtension(extension_name_str);
		if (load_result->HasError()) {
			throw Exception(ExceptionType::EXECUTOR, StringUtil::Format("Server failed to load extension %s: %s",
			                                                            extension_name_str, load_result->GetError()));
		}

		// Attempt to load extension on client side to keep client/server compatibility.
		try {
			ExtensionHelper::LoadExternalExtension(context, extension_name_str);
		} catch (std::exception &ex) {
			auto &db = DatabaseInstance::GetDatabase(context);
			DUCKDB_LOG_DEBUG(
			    db, StringUtil::Format("Failed to load extension %s because %s", extension_name_str, ex.what()));
		}

		return true;
	});
}

} // namespace duckdb
