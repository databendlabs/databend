#!/usr/bin/env bash
# Cluster parallel write regression for fixed-bucket partitioned PK tables.
# Requires a shared absolute local filesystem warehouse (prepared below).
# Must FAIL clearly if catalog/warehouse init fails — do not skip.
#
# Row-count assertions use $snapshots / $partitions metadata: a direct SELECT on
# Databend-written PK tables currently fails in paimon-0.2.0 sort-merge reorder
# ("must either specify a row count or at least one column"). Snapshot
# total_record_count is the PK-merged row count.

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# bendsql >=0.34 requires --query= in non-interactive mode.
sql() {
	bendsql_connect_root -n --query="$1"
}

WAREHOUSE_PATH="${PAIMON_WAREHOUSE_PATH:-${TESTS_DATA_DIR}/paimon_warehouse}"
if [[ ! -d "${WAREHOUSE_PATH}" ]]; then
	mkdir -p "${WAREHOUSE_PATH}" || {
		echo "FAIL: cannot create paimon warehouse path: ${WAREHOUSE_PATH}"
		exit 1
	}
fi
WAREHOUSE_PATH="$(cd "${WAREHOUSE_PATH}" && pwd)"

echo "===== prepare warehouse ====="
if ! "${CURDIR}"/../../../sqllogictests/scripts/prepare_paimon_fs_data.sh >/dev/null 2>&1; then
	echo "FAIL: paimon warehouse prepare failed"
	exit 1
fi
echo "prepare_ok"

sql "DROP CATALOG IF EXISTS paimon_fs" >/dev/null

if ! sql "CREATE CATALOG paimon_fs TYPE = PAIMON CONNECTION = (METASTORE = 'filesystem', WAREHOUSE = '${WAREHOUSE_PATH}')" >/dev/null; then
	echo "FAIL: paimon catalog init failed for warehouse ${WAREHOUSE_PATH}"
	exit 1
fi

# Probe that cluster write fixtures exist (fail, do not skip).
if ! sql "SELECT count(snapshot_id) FROM paimon_fs.regression.\"write_pk_part_b2\$snapshots\";" >/dev/null; then
	echo "FAIL: cluster write fixture write_pk_part_b2 not available"
	exit 1
fi

run_bucket_case() {
	local buckets="$1"
	local table="write_pk_part_b${buckets}"
	local fq="paimon_fs.regression.${table}"

	echo "===== bucket=${buckets} before snapshot ====="
	before=$(sql "SELECT count(snapshot_id) FROM paimon_fs.regression.\"${table}\$snapshots\";" | tail -n 1 | tr -d '[:space:]')
	echo "snapshots_before=${before}"

	echo "===== bucket=${buckets} insert ====="
	sql "INSERT INTO ${fq} SELECT number % 200, concat('v', number), number % 5 FROM numbers(100000);" >/dev/null
	echo "insert_ok"

	echo "===== bucket=${buckets} after snapshot ====="
	after=$(sql "SELECT count(snapshot_id) FROM paimon_fs.regression.\"${table}\$snapshots\";" | tail -n 1 | tr -d '[:space:]')
	echo "snapshots_after=${after}"
	delta=$((after - before))
	echo "snapshot_delta=${delta}"
	if [[ "${delta}" -ne 1 ]]; then
		echo "FAIL: expected snapshot_delta=1, got ${delta}"
		exit 1
	fi

	echo "===== bucket=${buckets} row counts ====="
	# PK-merged total from the new snapshot.
	sql "SELECT total_record_count FROM paimon_fs.regression.\"${table}\$snapshots\";"
	# Per-partition merged counts from partition stats.
	sql "SELECT \`partition\`, record_count FROM paimon_fs.regression.\"${table}\$partitions\" ORDER BY \`partition\`;"
}

for buckets in 2 8 64; do
	run_bucket_case "${buckets}"
done

# Lane observation (debug/test builds) returns Internal if the same route hits
# two lanes in one query. Reaching here means route→lane stayed a function.
echo "===== route to lane functional mapping ====="
echo "route_to_lane_functional=true"
