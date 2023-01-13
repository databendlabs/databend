#!/bin/bash

#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"

meta_dir="$SCRIPT_PATH/_meta_dir"
meta_json="$SCRIPT_PATH/meta-v23.txt"
exported="$SCRIPT_PATH/exported"

chmod +x ./target/${BUILD_PROFILE}/databend-meta-upgrade-2023-01-15

echo " === import into $meta_dir"
cat $meta_json |
    ./target/${BUILD_PROFILE}/databend-metactl --import --raft-dir "$meta_dir"

count_of_table_meta=$(cat "$meta_json" | wc -l)

sleep 1

echo " === upgrade"
./target/${BUILD_PROFILE}/databend-meta-upgrade-2023-01-15 --upgrade --raft-dir "$meta_dir"

echo " === export from $meta_dir"
./target/${BUILD_PROFILE}/databend-metactl --export --raft-dir "$meta_dir" >$exported

echo " === check affected lines"
count_of_diff=$(diff -y --suppress-common-lines "$meta_json" "$exported" | wc -l)
if [ "$count_of_table_meta" == "$count_of_diff" ]; then
    echo " === affected: $count_of_diff; OK"
else
    echo " === mismatching lines of upgraded: expect: $count_of_table_meta; got: $count_of_diff"
    exit 1
fi

echo " === check ver"
count_of_v24=$(./target/${BUILD_PROFILE}/databend-meta-upgrade-2023-01-15 --print --raft-dir "$meta_dir" | grep ' ver: 24' | wc -l)
if [ "$count_of_table_meta" == "$count_of_v24" ]; then
    echo " === count of ver=24: $count_of_v24; OK"
else
    echo " === mismatching lines of ver=24: expect: $count_of_table_meta; got: $count_of_v24"
    exit 1
fi
