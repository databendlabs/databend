#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

TABLE=t03_0017_fuse_recovery_blocks

echo "drop table if exists $TABLE all" | bendsql_connect_root_null
echo "create table $TABLE(id int, payload variant) cluster by(id) change_tracking=true enable_auto_analyze=0" | bendsql_connect_root_null
echo "insert into $TABLE values(1, parse_json('{\"v\":1}'))" | bendsql_connect_root_null

# UPDATE rewrites the block with Fuse stream-origin fields. Recovery must validate
# and strip those fields, then write a fresh INSERT block with current cluster stats.
echo "update $TABLE set payload = parse_json('{\"v\":2}') where id = 1" | bendsql_connect_root_null
BLOCK=$(echo "select _block_name from $TABLE" | bendsql_connect_root)

# The source block is now outside the current snapshot, but remains in _b/.
echo "truncate table $TABLE" | bendsql_connect_root_null
echo "copy into $TABLE from FUSE_RECOVERY_BLOCKS(FILES => ('$BLOCK')) force = false" | bendsql_connect_root_null
echo "recovered block"
echo "select count(), sum(id), max(payload['v']::int) from $TABLE" | bendsql_connect_root
echo "copy history has source size"
echo "select count() = 1 and min(content_length) > 0 from copy_history('$TABLE')" | bendsql_connect_root
RECOVERED_BLOCK=$(echo "select _block_name from $TABLE where id = 1" | bendsql_connect_root)

echo "cluster stats rebuilt"
echo "select count() > 0 and count_if(level is not null) = count() from clustering_statistics('default', '$TABLE')" | bendsql_connect_root

# Normal COPY history makes FORCE=FALSE retries idempotent.
echo "copy into $TABLE from FUSE_RECOVERY_BLOCKS(FILES => ('$BLOCK')) force = false" | bendsql_connect_root_null
echo "retry skipped"
echo "select count() from $TABLE" | bendsql_connect_root

# Remove the historical source block while retaining its COPY history entry.
# FORCE=FALSE must filter the path by history without touching object storage.
echo "set data_retention_time_in_days=0; optimize table $TABLE purge" | bendsql_connect_root_null
echo "copy into $TABLE from FUSE_RECOVERY_BLOCKS(FILES => ('$BLOCK')) force = false" | bendsql_connect_root_null
echo "purged history entry skipped"
echo "select count() from $TABLE" | bendsql_connect_root

# FORCE keeps the normal COPY semantics and attempts to read the purged block.
if echo "copy into $TABLE from FUSE_RECOVERY_BLOCKS(FILES => ('$BLOCK')) force = true" | bendsql_connect_root_null >/dev/null 2>&1; then
	echo "force reread of purged block unexpectedly succeeded"
else
	echo "force reread of purged block failed"
fi
echo "select count() from $TABLE" | bendsql_connect_root

# An unseen missing path is selected for recovery and must abort the statement;
# the history hit in the same FILES list remains filtered without a storage read.
MISSING="${BLOCK%/*}/missing_v2.parquet"
if echo "copy into $TABLE from FUSE_RECOVERY_BLOCKS(FILES => ('$BLOCK', '$MISSING')) force = false" | bendsql_connect_root_null >/dev/null 2>&1; then
	echo "missing file unexpectedly succeeded"
else
	echo "new missing file aborted batch"
fi
echo "select count() from $TABLE" | bendsql_connect_root

# One current-schema block plus one stale block must fail while all selected
# footers are validated, before either file can append data.
echo "alter table $TABLE add column added int" | bendsql_connect_root_null
echo "insert into $TABLE(id, payload, added) values(2, parse_json('{\"v\":3}'), 2)" | bendsql_connect_root_null
CURRENT_BLOCK=$(echo "select _block_name from $TABLE where id = 2" | bendsql_connect_root)
if echo "copy into $TABLE from FUSE_RECOVERY_BLOCKS(FILES => ('$CURRENT_BLOCK', '$RECOVERED_BLOCK')) force = true" | bendsql_connect_root_null >/dev/null 2>&1; then
	echo "schema mismatch unexpectedly succeeded"
else
	echo "schema mismatch aborted batch"
fi
echo "select count() from $TABLE" | bendsql_connect_root

echo "drop table $TABLE all" | bendsql_connect_root_null
