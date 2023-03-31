#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"

meta_dir="$SCRIPT_PATH/_meta_dir"
meta_json="$SCRIPT_PATH/meta.txt"
want_json="$SCRIPT_PATH/want.txt"
exported="$SCRIPT_PATH/exported"

chmod +x ./target/${BUILD_PROFILE}/databend-metactl

echo " === import into $meta_dir"
cat $meta_json |
    ./target/${BUILD_PROFILE}/databend-metactl --import --raft-dir "$meta_dir"

sleep 1

echo " === start a single node databend-meta"
# test export from grpc
chmod +x ./target/${BUILD_PROFILE}/databend-meta

# Give it a very big heartbeat interval to prevent election.
# Election will change the `vote` in storage and thus fail the following `diff`
# in this test.
./target/${BUILD_PROFILE}/databend-meta --heartbeat-interval 100000 --single --raft-dir "$meta_dir" &
METASRV_PID=$!
echo "meta-service pid:" $METASRV_PID
sleep 10

echo " === export data from a running databend-meta to $exported"
./target/${BUILD_PROFILE}/databend-metactl --export --grpc-api-address "localhost:9191" >$exported

echo " === exported file data start..."
cat $exported
echo " === exported file data end"

echo " === check backup date $want_json and exported $exported"
diff $want_json $exported

kill $METASRV_PID
