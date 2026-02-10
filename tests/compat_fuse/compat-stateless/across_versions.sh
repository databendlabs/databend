#!/usr/bin/env bash
set -euo pipefail

# Parameterized flashback / compaction compatibility test across format version boundaries.
#
# Usage: across_versions.sh <OLD_FMT_VER> <NEW_FMT_VER>
#
# OLD_FMT_VER — the snapshot format version written by the old query binary
# NEW_FMT_VER — the snapshot format version written by the current query binary

OLD_FMT_VER="${1:?Usage: across_versions.sh <OLD_FMT_VER> <NEW_FMT_VER>}"
NEW_FMT_VER="${2:?Usage: across_versions.sh <OLD_FMT_VER> <NEW_FMT_VER>}"

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../shell_env.sh

assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$expected" != "$actual" ]; then
        echo "FAIL: $desc"
        echo "  expected: $expected"
        echo "  actual:   $actual"
        exit 1
    fi
    echo "OK: $desc"
}

run_sql() {
    echo "$1" | $BENDSQL_CLIENT_CONNECT
}

##########################################
# test flashback across version boundary #
##########################################

echo "=== suite: flashback across version boundary (v${OLD_FMT_VER} -> v${NEW_FMT_VER}) ==="

# current situation:
# - we have inserted 3 rows into fuse_test_flashback, which produced 3 snapshots
# - and the content of the table is {1,2,3}

assert_eq "3 snapshots exist" \
    "3" \
    "$(run_sql "select count(*) from fuse_snapshot('default', 'fuse_test_flashback')")"

# write down 2 new rows using current version
run_sql "insert into fuse_test_flashback values (4)" > /dev/null
run_sql "insert into fuse_test_flashback values (5)" > /dev/null

# the table now contains five rows {1,2,3,4,5}, and 5 snapshots:
# s1 {1}, s2 {1,2}, s3 {1,2,3}, s4 {1,2,3,4}, s5 {1,2,3,4,5}
# s1, s2, s3 are of table snapshot format version $OLD_FMT_VER
# s4, s5 are of table snapshot format version $NEW_FMT_VER

assert_eq "5 snapshots exist" \
    "5" \
    "$(run_sql "select count(*) from fuse_snapshot('default', 'fuse_test_flashback')")"

# flashback to the 2nd last snapshot (s4, version $NEW_FMT_VER)
FST_SNAPSHOT_ID=$(run_sql "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=4")
run_sql "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')"

assert_eq "after flashback to s4 (v${NEW_FMT_VER}), table contains {1,2,3,4}" \
    "$(printf '1\n2\n3\n4')" \
    "$(run_sql "select c from fuse_test_flashback order by c")"

# flashback to s3 (version $NEW_FMT_VER, was upgraded from $OLD_FMT_VER)
FST_SNAPSHOT_ID=$(run_sql "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=3")
run_sql "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')"

assert_eq "after flashback to s3 (v${NEW_FMT_VER}), table contains {1,2,3}" \
    "$(printf '1\n2\n3')" \
    "$(run_sql "select c from fuse_test_flashback order by c")"

# crossing the version boundary

# flashback to s2 (version $OLD_FMT_VER)
FST_SNAPSHOT_ID=$(run_sql "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=2")
run_sql "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')"

assert_eq "after flashback to s2 (v${OLD_FMT_VER}), table contains {1,2}" \
    "$(printf '1\n2')" \
    "$(run_sql "select c from fuse_test_flashback order by c")"

# flashback to s1 (version $OLD_FMT_VER)
FST_SNAPSHOT_ID=$(run_sql "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=1")
run_sql "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')"

assert_eq "after flashback to s1 (v${OLD_FMT_VER}), table contains {1}" \
    "1" \
    "$(run_sql "select c from fuse_test_flashback order by c")"

###########################################################
# test compaction & fuse_snapshot across version boundary #
# issue 11204                                             #
###########################################################

echo "=== suite: compaction & fuse_snapshot across version boundary (v${OLD_FMT_VER} -> v${NEW_FMT_VER}) ==="

# current situation:
# - we have inserted 2 rows into fuse_test_compaction, which produced 2 snapshots
# - the content of the table is {1,2}
# - the snapshots are s1 {1}, s2 {1,2}

assert_eq "2 snapshots of version ${OLD_FMT_VER} exist" \
    "$(printf '2\t%s' "$OLD_FMT_VER")" \
    "$(run_sql "select count() c, format_version v from fuse_snapshot('default', 'fuse_test_compaction') group by v order by c")"

# grab the current last snapshot (s2)
FST_SNAPSHOT_S2_ID=$(run_sql "select snapshot_id from fuse_snapshot('default','fuse_test_compaction') limit 1")
# grab s2's location
FST_SNAPSHOT_S2_ID_LOC=$(run_sql "select snapshot_location from fuse_snapshot('default','fuse_test_compaction') limit 1")

# compact the table
echo "doing compact"
run_sql "optimize table fuse_test_compaction compact"

assert_eq "after compaction, table still contains {1,2}" \
    "$(printf '1\n2')" \
    "$(run_sql "select c from fuse_test_compaction order by c")"

assert_eq "after compaction, 2 snapshots of v${OLD_FMT_VER} and 1 of v${NEW_FMT_VER}" \
    "$(printf '1\t%s\n2\t%s' "$NEW_FMT_VER" "$OLD_FMT_VER")" \
    "$(run_sql "select count() c, format_version v from fuse_snapshot('default', 'fuse_test_compaction') group by v order by c")"

assert_eq "s2 location and version correct" \
    "$(printf 'true\ttrue')" \
    "$(run_sql "select snapshot_location='${FST_SNAPSHOT_S2_ID_LOC}', format_version=${OLD_FMT_VER} from fuse_snapshot('default', 'fuse_test_compaction') where snapshot_id='$FST_SNAPSHOT_S2_ID'")"

# flash back to s2
run_sql "alter table fuse_test_compaction flashback to (snapshot => '$FST_SNAPSHOT_S2_ID')"

assert_eq "flashback to s2 works" \
    "$(printf 'true\ttrue')" \
    "$(run_sql "select snapshot_location='${FST_SNAPSHOT_S2_ID_LOC}', format_version=${OLD_FMT_VER} from fuse_snapshot('default', 'fuse_test_compaction') limit 1")"

assert_eq "after flashback to s2, table contains {1,2}" \
    "$(printf '1\n2')" \
    "$(run_sql "select c from fuse_test_compaction order by c")"

###########################################
# mixed versioned segment compaction test #
###########################################

echo "=== suite: mixed versioned segment compaction (v${OLD_FMT_VER} -> v${NEW_FMT_VER}) ==="

# compaction of mixed versioned segments into new versioned segment
#
# current situation:
# - table option block_per_segment is 2
# - we have inserted 3 rows into t2, which produced 3 snapshots of version $OLD_FMT_VER
# - the snapshots are s1 {1}, s2 {1,2}, s3 {1,2,3}
# - after that, we perform a compaction on s3, which gives us a snapshot s4 {1,2,3}, of version $OLD_FMT_VER
#   since block_per_segment is 2, s4 contains 2 segments:
#     v${OLD_FMT_VER} segment_1: {1,2}, v${OLD_FMT_VER} segment_2: {3}

# creation of s5:
# insert another row, which will produce a new snapshot s5 {1,2,3,4}, of version $NEW_FMT_VER
run_sql "insert into t2 values (4)" > /dev/null

# s5 now contains 3 segments, 2 of version $OLD_FMT_VER, and 1 of version $NEW_FMT_VER

# creation of s6: the mixed version segments compaction
# compact the segments again.
# note: use `compact segment` here, otherwise `compact` would also compact blocks,
# producing two new segments of version $NEW_FMT_VER.
run_sql "optimize table t2 compact segment"

# the new snapshot s6 should contain 2 segments:
# - v${OLD_FMT_VER} segment_1: {1,2}, v${NEW_FMT_VER} segment_2: {3, 4}

# grab the id of s6
FST_SNAPSHOT_S6_ID=$(run_sql "select snapshot_id from fuse_snapshot('default','t2') limit 1")

assert_eq "after compaction, 2 segments: one v${OLD_FMT_VER} and one v${NEW_FMT_VER}" \
    "$(printf '1\t%s\n1\t%s' "$OLD_FMT_VER" "$NEW_FMT_VER")" \
    "$(run_sql "select count() c, format_version v from fuse_segment('default', 't2', '$FST_SNAPSHOT_S6_ID') group by v order by v")"

assert_eq "table contains {1,2,3,4} after compaction" \
    "$(printf '1\n2\n3\n4')" \
    "$(run_sql "select * from t2 order by c")"

echo "=== All stateless checks passed ==="
