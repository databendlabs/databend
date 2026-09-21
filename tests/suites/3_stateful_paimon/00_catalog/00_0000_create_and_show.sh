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

echo "===== catalog registered ====="
echo "SHOW CATALOGS LIKE 'paimon_fs';" | bendsql_connect_root

echo "===== show create catalog ====="
echo "SHOW CREATE CATALOG paimon_fs;" | bendsql_connect_root

echo "===== catalog databases ====="
cat <<'EOF' | bendsql_connect_root
USE CATALOG paimon_fs;
SHOW DATABASES;
EOF

echo "===== catalog tables ====="
cat <<'EOF' | bendsql_connect_root
USE CATALOG paimon_fs;
SHOW TABLES FROM regression;
EOF

echo "===== fully-qualified resolution (catalog -> db -> table) ====="
echo "SELECT count(*) FROM paimon_fs.regression.append_t;" | bendsql_connect_root

echo "===== read-only DDL rejected (database level) ====="
echo "CREATE DATABASE paimon_fs.new_db;" | bendsql_connect_root 2>&1 | grep -Ei "read-only|not supported"
