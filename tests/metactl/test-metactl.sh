#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"

meta_dir="$SCRIPT_PATH/_meta_dir"
meta_json="$SCRIPT_PATH/meta.json"
exported="$SCRIPT_PATH/exported"
grpc_exported="$SCRIPT_PATH/exported"

chmod +x ./target/debug/databend-metactl

cat $meta_json \
    | ./target/debug/databend-metactl --import --raft-dir "$meta_dir"

sleep 1

./target/debug/databend-metactl --export --raft-dir "$meta_dir" > $exported

diff $meta_json $exported

# test export from grpc
chmod +x ./target/debug/databend-meta
./target/debug/databend-meta --single &
METASRV_PID=$!
echo $METASRV_PID
sleep 0.5

./target/debug/databend-metactl --export --grpc-api-address "127.0.0.1:9191"  > $grpc_exported
grep -Fxq '["raft_state",{"RaftStateKV":{"key":"Id","value":{"NodeId":0}}}]' $grpc_exported

kill $METASRV_PID
