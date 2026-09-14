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

# System-table names contain '$'; queries are single-quoted so the shell leaves
# '$' untouched.

echo "===== branches ====="
echo 'SELECT count() FROM paimon_fs.regression."append_t$branches";' | bendsql_connect_root

echo "===== snapshots ====="
echo 'SELECT snapshot_id, schema_id, commit_kind FROM paimon_fs.regression."append_t$snapshots" ORDER BY snapshot_id;' | bendsql_connect_root

echo "===== schemas ====="
echo 'SELECT schema_id, partition_keys, primary_keys FROM paimon_fs.regression."append_t$schemas" ORDER BY schema_id;' | bendsql_connect_root

echo "===== options count ====="
echo 'SELECT count() FROM paimon_fs.regression."append_t$options";' | bendsql_connect_root

echo "===== files ====="
echo 'SELECT `partition`, record_count, file_format FROM paimon_fs.regression."append_t$files" ORDER BY 1;' | bendsql_connect_root

echo "===== files value stats ====="
echo 'SELECT `partition`, min_value_stats, max_value_stats FROM paimon_fs.regression."append_t$files" ORDER BY 1;' | bendsql_connect_root

echo "===== manifests count ====="
echo 'SELECT count() FROM paimon_fs.regression."append_t$manifests";' | bendsql_connect_root

echo "===== partitions ====="
echo 'SELECT `partition`, record_count, file_count FROM paimon_fs.regression."append_t$partitions" ORDER BY 1;' | bendsql_connect_root

echo "===== physical file counts ====="
echo 'SELECT manifest_file_count, data_file_count, index_file_count FROM paimon_fs.regression."append_t$physical_files_size";' | bendsql_connect_root

echo "===== referenced sources ====="
echo 'SELECT source FROM paimon_fs.regression."append_t$referenced_files_size" ORDER BY source;' | bendsql_connect_root

echo "===== table indexes count ====="
echo 'SELECT count() FROM paimon_fs.regression."append_t$table_indexes";' | bendsql_connect_root

echo "===== tags count ====="
echo 'SELECT count() FROM paimon_fs.regression."append_t$tags";' | bendsql_connect_root

echo "===== pk table keys ====="
echo 'SELECT min_key, max_key FROM paimon_fs.regression."pk_t$files" ORDER BY min_key;' | bendsql_connect_root

echo "===== unknown suffix rejected ====="
echo 'SELECT count() FROM paimon_fs.regression."append_t$unknown";' | bendsql_connect_root 2>&1 | grep "^Error:" | head -1
