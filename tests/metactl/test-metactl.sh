#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"

meta_dir="$SCRIPT_PATH/_meta_dir"
meta_json="$SCRIPT_PATH/meta.txt"
exported="$SCRIPT_PATH/exported"
grpc_exported="$SCRIPT_PATH/exported"

chmod +x ./target/${BUILD_PROFILE}/databend-metactl


echo " === import into $meta_dir"
cat $meta_json |
    ./target/${BUILD_PROFILE}/databend-metactl --import --raft-dir "$meta_dir"

sleep 1


echo " === export from $meta_dir"
./target/${BUILD_PROFILE}/databend-metactl --export --raft-dir "$meta_dir" >$exported


echo " === check backup date $meta_json and exported $exported"
diff $meta_json $exported


echo " === start a single node databend-meta"
# test export from grpc
chmod +x ./target/${BUILD_PROFILE}/databend-meta
./target/${BUILD_PROFILE}/databend-meta --single &
METASRV_PID=$!
echo $METASRV_PID
sleep 3


echo " === export data from a running databend-meta to $grpc_exported"
./target/${BUILD_PROFILE}/databend-metactl --export --grpc-api-address "127.0.0.1:9191" >$grpc_exported

echo " === exported file data start..."
cat $grpc_exported 
echo " === exported file data end"

echo " === check if there is a node record in it"
if grep -Fxq '["raft_state",{"RaftStateKV":{"key":"Id","value":{"NodeId":0}}}]' $grpc_exported;  then
    echo " === node record found, good!"
else
    echo " === No node record found!!!"
    exit 1
fi

kill $METASRV_PID
