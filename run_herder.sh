#!/usr/bin/env bash
set -euo pipefail

# Run the current Duckherder count(*) milestone against the stress-test Iceberg table.
# Existing EC2 instances are found by the stress-test Name tags; override the IDs or
# table with HERDER_DRIVER_INSTANCE_ID, HERDER_WORKER_INSTANCE_IDS, and HERDER_TABLE.
# The reference scripts recorded no live delete files for the default table.

export AWS_PAGER=""
export AWS_PROFILE="${AWS_PROFILE:-AdministratorAccess-859831564954}"
REGION="${AWS_DEFAULT_REGION:-us-west-2}"
BUCKET="${HERDER_BUCKET:-com-dbt-sandbox-iceberg-sandbox}"
PREFIX="${HERDER_S3_PREFIX:-quack_duckdb_stress_test}"
TABLE="${HERDER_TABLE:-iceberg_catalog.quack_stress_test.context_core__run_model}"
WORKER_COUNT="${HERDER_WORKER_COUNT:-4}"
WORKER_PORT="${HERDER_WORKER_PORT:-8816}"
DRIVER_PORT="${HERDER_DRIVER_PORT:-8815}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
REMOTE_DIR="/opt/run-herder/$RUN_ID"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${HERDER_BUILD_DIR:-$SCRIPT_DIR/build/release}"
DRY_RUN=false

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
  shift
fi
if [[ $# -ne 0 ]]; then
  echo "usage: $0 [--dry-run]" >&2
  exit 2
fi
if ! [[ "$TABLE" =~ ^[A-Za-z_][A-Za-z_0-9]*\.[A-Za-z_][A-Za-z_0-9]*\.[A-Za-z_][A-Za-z_0-9]*$ ]]; then
  echo "HERDER_TABLE must be a three-part unquoted Iceberg table name" >&2
  exit 2
fi
if ! [[ "$WORKER_COUNT" =~ ^[1-9][0-9]*$ && "$WORKER_COUNT" -le 32 ]]; then
  echo "HERDER_WORKER_COUNT must be between 1 and 32" >&2
  exit 2
fi
for port in "$WORKER_PORT" "$DRIVER_PORT"; do
  if ! [[ "$port" =~ ^[0-9]+$ && "$port" -ge 1 && "$port" -le 65535 ]]; then
    echo "ports must be between 1 and 65535" >&2
    exit 2
  fi
done
if ! [[ "$BUCKET" =~ ^[A-Za-z0-9.-]+$ && "$PREFIX" =~ ^[A-Za-z0-9_./-]+$ ]]; then
  echo "invalid HERDER_BUCKET or HERDER_S3_PREFIX" >&2
  exit 2
fi
PREFIX="${PREFIX#/}"
PREFIX="${PREFIX%/}"
if [[ -z "$PREFIX" ]]; then
  echo "HERDER_S3_PREFIX must not be empty" >&2
  exit 2
fi

aws_() { aws --region "$REGION" "$@"; }
sql_quote() {
  local value="${1//\'/\'\'}"
  printf "'%s'" "$value"
}

SCRATCH="$(mktemp -d)"
STARTED_IDS=()
WORKER_IDS=()
WORKER_IPS=()
WORKER_UNITS=()
NEW_SG_RULE_IDS=()
NEW_SG_RULE_GROUPS=()
BUNDLE_URI=""
QUERY_COMMAND_ID=""
DRIVER_ID=""
SSM_SEND_BACKEND=aws

cleanup() {
  local result=$?
  trap - EXIT
  if [[ "$DRY_RUN" == false ]]; then
    if [[ "$result" -ne 0 && -n "$QUERY_COMMAND_ID" ]]; then
      aws_ ssm cancel-command --command-id "$QUERY_COMMAND_ID" >/dev/null 2>&1 || true
    fi
    for i in "${!WORKER_UNITS[@]}"; do
      printf 'systemctl stop %q || true\n' "${WORKER_UNITS[$i]}" > "$SCRATCH/stop.sh"
      ssm_send "${WORKER_IDS[$i]}" "$SCRATCH/stop.sh" 120 >/dev/null 2>&1 || true
    done
    for i in "${!NEW_SG_RULE_IDS[@]}"; do
      aws_ ec2 revoke-security-group-ingress --group-id "${NEW_SG_RULE_GROUPS[$i]}" \
        --security-group-rule-ids "${NEW_SG_RULE_IDS[$i]}" >/dev/null 2>&1 || true
    done
    if [[ -n "$BUNDLE_URI" ]]; then
      aws_ s3 rm "$BUNDLE_URI" >/dev/null 2>&1 || true
    fi
    if [[ "${#STARTED_IDS[@]}" -gt 0 ]]; then
      aws_ ec2 stop-instances --instance-ids "${STARTED_IDS[@]}" >/dev/null 2>&1 || true
      echo "Stopped EC2 instances started by this run: ${STARTED_IDS[*]}"
    fi
  fi
  rm -rf "$SCRATCH"
  exit "$result"
}
trap cleanup EXIT

lookup_instance() {
  local tag="$1"
  local matches
  matches=$(aws_ ec2 describe-instances \
    --filters "Name=tag:Name,Values=$tag" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].InstanceId' --output text)
  local ids=($matches)
  if [[ "${#ids[@]}" -ne 1 ]]; then
    echo "Expected one EC2 instance tagged Name=$tag; found ${#ids[@]}. Set explicit instance IDs or provision the stress-test fleet." >&2
    return 1
  fi
  printf '%s\n' "${ids[0]}"
}

start_instance() {
  local id="$1" state
  state=$(aws_ ec2 describe-instances --instance-ids "$id" --query 'Reservations[0].Instances[0].State.Name' --output text)
  if [[ "$state" == stopping ]]; then
    aws_ ec2 wait instance-stopped --instance-ids "$id"
    state=stopped
  fi
  if [[ "$state" == stopped ]]; then
    aws_ ec2 start-instances --instance-ids "$id" >/dev/null
    STARTED_IDS+=("$id")
  elif [[ "$state" != running && "$state" != pending ]]; then
    echo "EC2 instance $id is $state" >&2
    return 1
  fi
  aws_ ec2 wait instance-running --instance-ids "$id"
  local status
  for _ in {1..36}; do
    status=$(aws_ ssm describe-instance-information --filters "Key=InstanceIds,Values=$id" \
      --query 'InstanceInformationList[0].PingStatus' --output text)
    if [[ "$status" == Online ]]; then
      return 0
    fi
    sleep 5
  done
  echo "SSM agent did not come online on $id" >&2
  return 1
}

instance_field() {
  aws_ ec2 describe-instances --instance-ids "$1" --query "$2" --output text
}

check_ssm_sender() {
  local error
  if error=$(aws_ ssm send-command --generate-cli-skeleton input 2>&1); then
    return 0
  fi
  if [[ "$error" == *"badly formed help string"* ]] && python3 - <<'PY'
try:
    from botocore.session import Session
except ModuleNotFoundError:
    from awscli.botocore.session import Session
PY
  then
    SSM_SEND_BACKEND=botocore
    echo "Using Python botocore for SSM send-command (installed AWS CLI cannot parse this command)"
    return 0
  fi
  printf 'SSM send-command is unavailable: %s\n' "$error" >&2
  return 1
}

ssm_send() {
  local id="$1" command_file="$2" timeout="$3"
  python3 - "$id" "$command_file" "$timeout" "$SCRATCH/ssm.json" <<'PY'
import json, pathlib, sys
instance_id, command_file, timeout, output = sys.argv[1:]
payload = {
    "DocumentName": "AWS-RunShellScript",
    "InstanceIds": [instance_id],
    "Parameters": {
        "commands": [pathlib.Path(command_file).read_text()],
        "executionTimeout": [timeout],
    },
    "TimeoutSeconds": 600,
}
pathlib.Path(output).write_text(json.dumps(payload))
PY
  if [[ "$SSM_SEND_BACKEND" == botocore ]]; then
    python3 - "$SCRATCH/ssm.json" "$REGION" <<'PY'
import json, os, sys
try:
    from botocore.session import Session
except ModuleNotFoundError:
    from awscli.botocore.session import Session
with open(sys.argv[1]) as input_file:
    request = json.load(input_file)
client = Session(profile=os.environ.get("AWS_PROFILE")).create_client("ssm", region_name=sys.argv[2])
print(client.send_command(**request)["Command"]["CommandId"])
PY
  else
    aws_ ssm send-command --cli-input-json "file://$SCRATCH/ssm.json" --query 'Command.CommandId' --output text
  fi
}

ssm_wait() {
  local id="$1" command_id="$2" timeout="$3" start status output error
  start=$(date +%s)
  while true; do
    status=$(aws_ ssm get-command-invocation --command-id "$command_id" --instance-id "$id" \
      --query Status --output text 2>/dev/null || true)
    case "$status" in
      Success|Failed|Cancelled|TimedOut|DeliveryTimedOut|ExecutionTimedOut) break ;;
    esac
    if (( $(date +%s) - start > timeout + 600 )); then
      echo "SSM command $command_id exceeded ${timeout}s" >&2
      return 1
    fi
    if (( ($(date +%s) - start) % 60 < 5 )); then
      echo "Waiting for $id ($status, $(( $(date +%s) - start ))s)"
    fi
    sleep 5
  done
  output=$(aws_ ssm get-command-invocation --command-id "$command_id" --instance-id "$id" \
    --query StandardOutputContent --output text)
  error=$(aws_ ssm get-command-invocation --command-id "$command_id" --instance-id "$id" \
    --query StandardErrorContent --output text)
  [[ "$output" == None ]] || printf '%s\n' "$output"
  [[ "$error" == None ]] || printf '%s\n' "$error" >&2
  if [[ "$status" != Success ]]; then
    echo "SSM command $command_id on $id failed: $status" >&2
    return 1
  fi
}

render_sql() {
  local endpoint="$1" database_uri="localhost:$DRIVER_PORT/herder_$RUN_ID.db"
  local data_path="s3://$BUCKET/$PREFIX"
  local data_path_sql database_uri_sql table_sql attach_sql s3_endpoint
  data_path_sql=$(sql_quote "$data_path")
  database_uri_sql=$(sql_quote "$database_uri")
  table_sql=$(sql_quote "SELECT count(*) FROM $TABLE")
  s3_endpoint="${HERDER_S3_ENDPOINT:-s3.$REGION.amazonaws.com}"
  cat > "$SCRATCH/run.sql" <<EOF
LOAD httpfs;
LOAD aws;
CREATE SECRET herder_s3 (TYPE S3, PROVIDER credential_chain, CHAIN 'instance', REGION $(sql_quote "$REGION"), ENDPOINT $(sql_quote "$s3_endpoint"), SCOPE $data_path_sql);
SELECT duckherder_start_local_server($DRIVER_PORT, 0);
EOF
  for i in "${!WORKER_IPS[@]}"; do
    printf "SELECT duckherder_register_worker('worker-%s', 'grpc://%s:%s');\n" \
      "$i" "${WORKER_IPS[$i]}" "$WORKER_PORT" >> "$SCRATCH/run.sql"
  done
  cat >> "$SCRATCH/run.sql" <<EOF
ATTACH DATABASE $database_uri_sql AS dh (TYPE duckherder, DATA_PATH $data_path_sql, SECRET 'herder_s3');
PRAGMA duckherder_query('LOAD httpfs');
PRAGMA duckherder_query('LOAD iceberg');
EOF
  attach_sql="ATTACH '' AS iceberg_catalog (TYPE ICEBERG, ENDPOINT $(sql_quote "$endpoint"), AUTHORIZATION_TYPE 'none')"
  printf 'PRAGMA duckherder_query(%s);\n' "$(sql_quote "$attach_sql")" >> "$SCRATCH/run.sql"
  cat >> "$SCRATCH/run.sql" <<EOF
ATTACH DATABASE $database_uri_sql AS rd (TYPE duckherder, READ_ONLY, DATA_PATH $data_path_sql, SECRET 'herder_s3');
DETACH dh;
ATTACH DATABASE $database_uri_sql AS dh (TYPE duckherder, READ_ONLY, DATA_PATH $data_path_sql, SECRET 'herder_s3');
SELECT 'COUNT_RESULT' AS metric, * FROM duckherder_query($table_sql);
SELECT execution_mode, num_workers_used, num_tasks_generated FROM duckherder_get_query_execution_stats() WHERE sql = $table_sql;
EOF
}

if [[ "$DRY_RUN" == true ]]; then
  for (( i=0; i<WORKER_COUNT; i++ )); do WORKER_IPS+=("10.0.0.$((10+i))"); done
  render_sql "${HERDER_CATALOG_ENDPOINT:-http://10.0.0.99:8181}"
  echo "Driver tag: quack-duckdb-stress-test"
  echo "Worker tags: quack-shard-worker-0..$((WORKER_COUNT-1))"
  echo "S3 bundle: s3://$BUCKET/$PREFIX/run_herder/$RUN_ID/bundle.tar.gz"
  cat "$SCRATCH/run.sql"
  exit 0
fi

for tool in aws cmake python3 tar; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done
QUERY_TIMEOUT="${HERDER_QUERY_TIMEOUT_SECONDS:-5400}"
if ! [[ "$QUERY_TIMEOUT" =~ ^[1-9][0-9]*$ && "$QUERY_TIMEOUT" -le 172800 ]]; then
  echo "HERDER_QUERY_TIMEOUT_SECONDS must be between 1 and 172800" >&2
  exit 2
fi
if [[ "$(uname -s)" != Linux || "$(uname -m)" != x86_64 ]]; then
  echo "Build and EC2 instances must both be Linux x86_64" >&2
  exit 1
fi
check_ssm_sender

DRIVER_ID="${HERDER_DRIVER_INSTANCE_ID:-$(lookup_instance quack-duckdb-stress-test)}"
if [[ -n "${HERDER_WORKER_INSTANCE_IDS:-}" ]]; then
  IFS=',' read -r -a WORKER_IDS <<< "$HERDER_WORKER_INSTANCE_IDS"
  if [[ "${#WORKER_IDS[@]}" -ne "$WORKER_COUNT" ]]; then
    echo "HERDER_WORKER_INSTANCE_IDS must contain $WORKER_COUNT comma-separated IDs" >&2
    exit 2
  fi
else
  for (( i=0; i<WORKER_COUNT; i++ )); do
    WORKER_IDS+=("$(lookup_instance "quack-shard-worker-$i")")
  done
fi
if [[ -n "${HERDER_CATALOG_ENDPOINT:-}" ]]; then
  CATALOG_ENDPOINT="$HERDER_CATALOG_ENDPOINT"
else
  catalog_id=$(lookup_instance quack-iceberg-catalog)
  catalog_state=$(instance_field "$catalog_id" 'Reservations[0].Instances[0].State.Name')
  if [[ "$catalog_state" != running ]]; then
    echo "Iceberg catalog $catalog_id is $catalog_state; start it first" >&2
    exit 1
  fi
  catalog_ip=$(instance_field "$catalog_id" 'Reservations[0].Instances[0].PrivateIpAddress')
  CATALOG_ENDPOINT="http://$catalog_ip:8181"
fi

echo "Starting driver $DRIVER_ID and ${#WORKER_IDS[@]} workers"
for id in "$DRIVER_ID" "${WORKER_IDS[@]}"; do
  arch=$(instance_field "$id" 'Reservations[0].Instances[0].Architecture')
  if [[ "$arch" != x86_64 ]]; then
    echo "EC2 instance $id is $arch; this build requires x86_64" >&2
    exit 1
  fi
done
start_instance "$DRIVER_ID"
for id in "${WORKER_IDS[@]}"; do start_instance "$id"; done

driver_sg=$(instance_field "$DRIVER_ID" 'Reservations[0].Instances[0].SecurityGroups[0].GroupId')
for id in "${WORKER_IDS[@]}"; do
  worker_sg=$(instance_field "$id" 'Reservations[0].Instances[0].SecurityGroups[0].GroupId')
  if rule_id=$(aws_ ec2 authorize-security-group-ingress --group-id "$worker_sg" \
      --protocol tcp --port "$WORKER_PORT" --source-group "$driver_sg" \
      --query 'SecurityGroupRules[0].SecurityGroupRuleId' --output text 2>"$SCRATCH/sg-error"); then
    NEW_SG_RULE_IDS+=("$rule_id")
    NEW_SG_RULE_GROUPS+=("$worker_sg")
  else
    error=$(cat "$SCRATCH/sg-error")
    if [[ "$error" != *InvalidPermission.Duplicate* ]]; then
      printf '%s\n' "$error" >&2
      exit 1
    fi
  fi
  WORKER_IPS+=("$(instance_field "$id" 'Reservations[0].Instances[0].PrivateIpAddress')")
done
render_sql "$CATALOG_ENDPOINT"

echo "Building DuckDB and standalone worker"
cmake --build "$BUILD_DIR" --target shell distributed_worker -j "${HERDER_BUILD_JOBS:-4}" >/dev/null
mkdir -p "$SCRATCH/bundle/bin" "$SCRATCH/bundle/extensions"
cp "$BUILD_DIR/duckdb" "$SCRATCH/bundle/bin/duckdb"
cp "$BUILD_DIR/extension/duckherder/distributed_worker" "$SCRATCH/bundle/bin/distributed_worker"
cp "$SCRATCH/run.sql" "$SCRATCH/bundle/run.sql"
"$BUILD_DIR/duckdb" -bail -c "SET extension_directory=$(sql_quote "$SCRATCH/bundle/extensions"); INSTALL httpfs; INSTALL aws; INSTALL iceberg;" >/dev/null
tar -czf "$SCRATCH/bundle.tar.gz" -C "$SCRATCH/bundle" .
BUNDLE_URI="s3://$BUCKET/$PREFIX/run_herder/$RUN_ID/bundle.tar.gz"
echo "Uploading $(du -h "$SCRATCH/bundle.tar.gz" | cut -f1) bundle to $BUNDLE_URI"
aws_ s3 cp "$SCRATCH/bundle.tar.gz" "$BUNDLE_URI" >/dev/null
presigned_url=$(aws_ s3 presign "$BUNDLE_URI" --expires-in 3600)
if [[ "$presigned_url" == *\'* ]]; then
  echo "Unexpected quote in presigned URL" >&2
  exit 1
fi
cat > "$SCRATCH/install.sh" <<EOF
set -eu
mkdir -p '$REMOTE_DIR'
python3 - '$presigned_url' '$REMOTE_DIR/bundle.tar.gz' <<'PY'
import sys, urllib.request
urllib.request.urlretrieve(sys.argv[1], sys.argv[2])
PY
tar -xzf '$REMOTE_DIR/bundle.tar.gz' -C '$REMOTE_DIR'
mkdir -p /root/.duckdb/extensions
cp -a '$REMOTE_DIR/extensions/.' /root/.duckdb/extensions/
rm '$REMOTE_DIR/bundle.tar.gz'
export HOME=/root
'$REMOTE_DIR/bin/duckdb' -bail -c 'LOAD httpfs; LOAD aws; LOAD iceberg;'
EOF

echo "Copying DuckDB, worker, and matching aws/httpfs/iceberg extensions to EC2"
for id in "$DRIVER_ID" "${WORKER_IDS[@]}"; do
  command_id=$(ssm_send "$id" "$SCRATCH/install.sh" 1200)
  ssm_wait "$id" "$command_id" 1200
done

for i in "${!WORKER_IDS[@]}"; do
  unit="duckherder-$RUN_ID-worker-$i"
  WORKER_UNITS[$i]="$unit"
  cat > "$SCRATCH/start.sh" <<EOF
set -eu
systemd-run --unit='$unit' --collect --setenv=HOME=/root --property=LimitNOFILE=65535 \
  '$REMOTE_DIR/bin/distributed_worker' 0.0.0.0 '$WORKER_PORT' 'worker-$i'
for attempt in \$(seq 1 30); do
  if systemctl is-active --quiet '$unit' && \
      python3 -c "import socket; socket.create_connection(('127.0.0.1', $WORKER_PORT), 1).close()" 2>/dev/null; then
    exit 0
  fi
  sleep 1
done
systemctl status '$unit' --no-pager
exit 1
EOF
  command_id=$(ssm_send "${WORKER_IDS[$i]}" "$SCRATCH/start.sh" 120)
  ssm_wait "${WORKER_IDS[$i]}" "$command_id" 120
done

cat > "$SCRATCH/query.sh" <<EOF
set -eu
export HOME=/root
ulimit -n 65535 || true
'$REMOTE_DIR/bin/duckdb' -bail -csv -f '$REMOTE_DIR/run.sql'
EOF
echo "Running SELECT count(*) FROM $TABLE via $CATALOG_ENDPOINT"
QUERY_COMMAND_ID=$(ssm_send "$DRIVER_ID" "$SCRATCH/query.sh" "$QUERY_TIMEOUT")
ssm_wait "$DRIVER_ID" "$QUERY_COMMAND_ID" "$QUERY_TIMEOUT"
