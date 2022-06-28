#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"

meta_dir="$SCRIPT_PATH/_meta_dir"
import_meta_dir="$SCRIPT_PATH/_import_meta_dir"
meta_json="$SCRIPT_PATH/meta.json"
exported="$SCRIPT_PATH/exported"
grpc_exported="$SCRIPT_PATH/exported"

chmod +x ./target/${BUILD_PROFILE}/databend-metactl

#killall databend-meta

echo " === import into $meta_dir"
cat $meta_json |
    ./target/${BUILD_PROFILE}/databend-metactl --import --raft-dir "$meta_dir"

sleep 1


echo " === export from $meta_dir"
./target/${BUILD_PROFILE}/databend-metactl --export --raft-dir "$meta_dir" --db $exported


echo " === check backup date $meta_json and exported $exported"
diff $meta_json $exported


echo " === start a single node databend-meta"
# test export from grpc
chmod +x ./target/${BUILD_PROFILE}/databend-meta
./target/${BUILD_PROFILE}/databend-meta --single &
METASRV_PID=$!
echo $METASRV_PID
sleep 1


echo " === export data from a running databend-meta to $grpc_exported"
./target/${BUILD_PROFILE}/databend-metactl --export --grpc-api-address "127.0.0.1:9191" --db $grpc_exported

echo " === exported file data start..."
cat $grpc_exported 
echo " === exported file data end"

echo " === check if there is a node record in it"
grep -Fxq '["raft_state",{"RaftStateKV":{"key":"Id","value":{"NodeId":0}}}]' $grpc_exported

kill $METASRV_PID

echo " === export data from a running databend-meta to $grpc_exported with initial-cluster args"
./target/${BUILD_PROFILE}/databend-metactl --import --raft-dir $import_meta_dir --db $grpc_exported --initial-cluster 3=http://localhost:23822,0.0.0.0:9292

./target/${BUILD_PROFILE}/databend-metactl --export --raft-dir $import_meta_dir --db $grpc_exported
echo " === exported file data with initial-cluster args start..."
cat $grpc_exported 
echo " === exported file data with initial-cluster args end"

echo " === check if there is a initial-cluster node record in it"
grep -Fxq '["state_machine/0",{"Nodes":{"key":3,"value":{"name":"3","endpoint":{"addr":"localhost","port":23822},"grpc_api_addr":"0.0.0.0:9292"}}}]' $grpc_exported
grep -Fq '"membership":{"configs":[[3]],"all_nodes":[3]}' $grpc_exported

