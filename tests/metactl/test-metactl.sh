#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"

meta_dir="$SCRIPT_PATH/_meta_dir"
meta_json="$SCRIPT_PATH/meta.json"
exported="$SCRIPT_PATH/exported"
grpc_exported="$SCRIPT_PATH/exported"

chmod +x ./target/${BUILD_PROFILE}/databend-metactl

cat $meta_json |
    ./target/${BUILD_PROFILE}/databend-metactl --import --raft-dir "$meta_dir"

sleep 1

./target/${BUILD_PROFILE}/databend-metactl --export --raft-dir "$meta_dir" >$exported

diff $meta_json $exported

# test export from grpc
chmod +x ./target/${BUILD_PROFILE}/databend-meta
./target/${BUILD_PROFILE}/databend-meta --single &
METASRV_PID=$!
echo $METASRV_PID
sleep 0.5

./target/${BUILD_PROFILE}/databend-metactl --export --grpc-api-address "127.0.0.1:9191" >$grpc_exported
grep -Fxq '["raft_state",{"RaftStateKV":{"key":"Id","value":{"NodeId":0}}}]' $grpc_exported

kill $METASRV_PID
