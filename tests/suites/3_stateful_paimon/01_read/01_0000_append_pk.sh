#!/usr/bin/env bash

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
EOF

echo "===== append table full scan ====="
echo "SELECT id, name FROM paimon_fs.regression.append_t ORDER BY id;" | bendsql_connect_root

echo "===== pk table deduplicated scan ====="
echo "SELECT id, name FROM paimon_fs.regression.pk_t ORDER BY id;" | bendsql_connect_root

echo "===== projection and filter with limit ====="
echo "SELECT id FROM paimon_fs.regression.append_t WHERE id >= 2 ORDER BY id LIMIT 2;" | bendsql_connect_root

echo "===== read-only DDL in paimon catalog ====="
echo "CREATE TABLE paimon_fs.regression.new_t(id int);" | bendsql_connect_root 2>&1 | grep -Ei "read-only|not supported"
