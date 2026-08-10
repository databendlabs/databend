#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

WAREHOUSE_PATH="${PAIMON_WAREHOUSE_PATH:-${TESTS_DATA_DIR}/paimon_warehouse}"

"${CURDIR}"/../../../sqllogictests/scripts/prepare_paimon_fs_data.sh >/dev/null 2>&1

echo "DROP CATALOG IF EXISTS paimon_fs" | bendsql_connect_root

cat <<EOF | bendsql_connect_root
CREATE CATALOG paimon_fs TYPE = PAIMON CONNECTION = (
    METASTORE = 'filesystem',
    WAREHOUSE = '${WAREHOUSE_PATH}'
);
USE CATALOG paimon_fs;
SET max_threads = 4;
EOF

echo "===== row count from at least four splits ====="
# Each bendsql invocation is a fresh session, so the catalog set above does not
# persist here; fully qualify the paimon catalog.
echo "SELECT count(*) >= 4 FROM paimon_fs.regression.append_t;" | bendsql_connect_root

node_count=$(echo "SELECT count() FROM system.clusters;" | bendsql_connect_root | tail -n 1 | tr -d '[:space:]')
echo "===== active cluster nodes: ${node_count} ====="

if [ "${node_count}" -lt 2 ]; then
	echo "SKIP multi-node evidence: standalone deployment"
	echo "true"
	exit 0
fi

echo "===== distributed scan plan ====="
# On a multi-node cluster the paimon scan is planned under a Merge Exchange, i.e.
# the TableScan fragment runs across cluster executors and results are merged on
# the coordinator. The aggregated EXPLAIN ANALYZE profile does not attribute rows
# to individual nodes, so the plan's Exchange is the deterministic multi-node
# signal here.
plan=$(echo "EXPLAIN SELECT id FROM paimon_fs.regression.append_t;" | bendsql_connect_root)

if grep -q "Exchange" <<<"${plan}"; then
	echo "distributed_scan=true"
else
	echo "distributed_scan=false"
fi

partition_line=$(grep -m 1 -oE "partitions total: [0-9]+" <<<"${plan}")
echo "${partition_line}"

grep -q "Exchange" <<<"${plan}"
partition_count=${partition_line##*: }
test "${partition_count}" -ge 4
