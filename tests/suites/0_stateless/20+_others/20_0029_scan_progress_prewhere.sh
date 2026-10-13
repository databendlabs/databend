#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# Scan progress must report the rows and bytes the scan read and decoded,
# not the rows left after prewhere / runtime filters were applied inside
# the scan. One block of 1000 rows is created so that no block pruning
# happens for the filters below.
cat <<EOF | bendsql_connect_root > /dev/null
create or replace database scan_progress_prewhere;
create or replace table scan_progress_prewhere.t(a int, b string) block_size_threshold = 1073741824;
insert into scan_progress_prewhere.t select number, repeat('x', 10) from numbers(1000);
create or replace table scan_progress_prewhere.vt(id int, val json) storage_format = 'parquet' enable_virtual_column = true block_size_threshold = 1073741824;
insert into scan_progress_prewhere.vt select number, parse_json('{"a":' || number || ',"c":"' || repeat('y', 20) || '"}') from numbers(1000);
EOF

# bendsql does not expose the scan progress, so query via HTTP and read .stats.
run_query() {
	curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
		--header 'Content-Type: application/json' \
		-d "{\"sql\": \"$1\", \"pagination\": { \"wait_time_secs\": 6}}" \
		| jq -r '.stats.scan_progress.rows, (.stats.scan_progress.bytes > 0), .error'
}

echo "prewhere over all projected columns: expects scan rows 1000, bytes > 0, no error"
run_query "select count() from scan_progress_prewhere.t where a = 5"

echo "prewhere with remaining columns: expects scan rows 1000, bytes > 0, no error"
run_query "select b from scan_progress_prewhere.t where a = 5"

# The filter is not derivable from the column statistics, so the block is not
# pruned and the scan has to decode it even though no row survives.
echo "prewhere filtering out every row: expects scan rows 1000, bytes > 0, no error"
run_query "select b from scan_progress_prewhere.t where to_string(a) = 'nope'"

# Virtual columns are materialized after prewhere; they must still count.
scan_bytes() {
	curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
		--header 'Content-Type: application/json' \
		-d "{\"sql\": \"$1\", \"pagination\": { \"wait_time_secs\": 6}}" \
		| jq -r '.stats.scan_progress.bytes'
}
ID_ONLY_BYTES=$(scan_bytes "select id from scan_progress_prewhere.vt where id = 5")
VIRTUAL_BYTES=$(scan_bytes "select val['c'] from scan_progress_prewhere.vt where id = 5")
echo "virtual column after prewhere: expects scan rows 1000 and more bytes than the id-only scan"
run_query "select val['c'] from scan_progress_prewhere.vt where id = 5" | head -1
[ "$VIRTUAL_BYTES" -gt "$ID_ONLY_BYTES" ] && echo "true" || echo "false: virtual=$VIRTUAL_BYTES id_only=$ID_ONLY_BYTES"

echo "drop database scan_progress_prewhere;" | bendsql_connect_root
