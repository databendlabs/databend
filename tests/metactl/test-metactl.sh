#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"

meta_dir="$SCRIPT_PATH/_meta_dir"
meta_json="$SCRIPT_PATH/meta.txt"
want_exported="$SCRIPT_PATH/want_exported"

exported="$SCRIPT_PATH/exported"
grpc_exported="$SCRIPT_PATH/grpc_exported"

chmod +x ./target/${BUILD_PROFILE}/databend-metactl

echo " === "
echo " === 1. Test import $meta_json into dir: $meta_dir"
echo " === "

echo " === import into $meta_dir"
cat $meta_json |
    ./target/${BUILD_PROFILE}/databend-metactl --import --raft-dir "$meta_dir"

sleep 1

echo " === "
echo " === 2. Test export from dir: $meta_dir to file $exported"
echo " === "


echo " === export from $meta_dir"
./target/${BUILD_PROFILE}/databend-metactl --export --raft-dir "$meta_dir" >$exported

echo " === check backup date: $want_exported and exported: $exported"
diff $want_exported $exported


echo " === "
echo " === 3. Test export from running metasrv to file $grpc_exported"
echo " === "


echo " === start a single node databend-meta"
# test export from grpc
chmod +x ./target/${BUILD_PROFILE}/databend-meta
./target/${BUILD_PROFILE}/databend-meta --single &
METASRV_PID=$!
echo $METASRV_PID
sleep 10

echo " === export data from a running databend-meta to $grpc_exported"
./target/${BUILD_PROFILE}/databend-metactl --export --grpc-api-address "localhost:9191" >$grpc_exported

echo " === grpc_exported file data start..."
cat $grpc_exported
echo " === grpc_exported file data end"

echo " === check if there is a node record in it"
if grep -Fxq '["raft_state",{"RaftStateKV":{"key":"Id","value":{"NodeId":0}}}]' $grpc_exported; then
    echo " === Node record found, good!"
else
    echo " === No Node record found!!!"
    exit 1
fi

echo " === check if there is a header record in it"
if grep -Fxq '["header",{"DataHeader":{"key":"header","value":{"version":"V001","upgrading":null}}}]' $grpc_exported; then
    echo " === Header record found, good!"
else
    echo " === No Header record found!!!"
    exit 1
fi


kill $METASRV_PID
sleep 3


echo " === "
echo " === 4. Test import data with header $grpc_exported to dir $meta_dir"
echo " === "

echo " === import into $meta_dir"
cat $grpc_exported |
    ./target/${BUILD_PROFILE}/databend-metactl --import --raft-dir "$meta_dir"


echo " === "
echo " === 5. Test export data with header from dir: $meta_dir to file $exported"
echo " === "

echo " === export from $meta_dir"
./target/${BUILD_PROFILE}/databend-metactl --export --raft-dir "$meta_dir" >$exported

echo " === exported file data start..."
cat $exported
echo " === exported file data end"

echo " === check backup data $grpc_exported and exported $exported"
diff $grpc_exported $exported



echo " === "
echo " === 6. Test import data with incompatible header $grpc_exported to dir $meta_dir"
echo " === "


echo '["header",{"DataHeader":{"key":"header","value":{"version":"V100","upgrading":null}}}]' > $grpc_exported

echo " === import into $meta_dir"
cat $grpc_exported |
    ./target/${BUILD_PROFILE}/databend-metactl --import --raft-dir "$meta_dir"  \
    && { echo " === expect error when importing incompatible header"; exit 1; } \
    || echo " === error is expected. OK";


