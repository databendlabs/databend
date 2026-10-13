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

echo "drop database scan_progress_prewhere;" | bendsql_connect_root
