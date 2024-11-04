#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"

meta_dir="$SCRIPT_PATH/_meta_dir"
meta_json_v002="$SCRIPT_PATH/meta_v002.txt"
want_exported_v003="$SCRIPT_PATH/want_exported_v003"
want_snapshot_v003="$SCRIPT_PATH/want_snapshot_v003"

exported="$SCRIPT_PATH/exported"
grpc_exported="$SCRIPT_PATH/grpc_exported"

chmod +x ./target/${BUILD_PROFILE}/databend-metactl
chmod +x ./target/${BUILD_PROFILE}/databend-meta

# rm -rf "$meta_dir" || echo "Skip rm: $meta_dir"

metactl_import_export () {
    local ver="$1"
    local src="$2"
    local want_exported="$3"
    local want_snapshot="$4"

    local title="$ver"

    echo " === "
    echo " === ${title} 1. Test import $src into dir: $meta_dir"
    echo " === "

    echo " === import into $meta_dir"
    cat $src |
        ./target/${BUILD_PROFILE}/databend-metactl import --raft-dir "$meta_dir"

    sleep 1

    echo " === "
    echo " === ${title} 1.1. Check snapshot data"
    echo " === "

    snapshot_path="$(ls $meta_dir/df_meta/V003/snapshot/1-0-83-*.snap)"
    echo "=== snapshot path:"
    ls $snapshot_path

    echo "=== check snapshot content"
    diff "$want_snapshot" "$snapshot_path"


    echo " === "
    echo " === ${title} 2. Test export from dir: $meta_dir to file $exported"
    echo " === "

    echo " === export from $meta_dir"
    ./target/${BUILD_PROFILE}/databend-metactl export --raft-dir "$meta_dir" >$exported

    echo " === check backup date: $want_exported and exported: $exported"
    diff $want_exported $exported

    echo " === "
    echo " === ${title} 3. Test export from running meta-service to file $grpc_exported"
    echo " === "

    echo " === start databend-meta"
    # Give it a very big heartbeat interval to prevent election.
    # Election will change the `vote` in storage and thus fail the following `diff`
    # in this test.
    ./target/${BUILD_PROFILE}/databend-meta --single --heartbeat-interval 100000 --raft-dir "$meta_dir" --log-file-level=debug &
    METASRV_PID=$!
    echo " === pid: $METASRV_PID"
    sleep 10

    echo " === export from running databend-meta to $grpc_exported"
    ./target/${BUILD_PROFILE}/databend-metactl export --grpc-api-address "localhost:9191" >$grpc_exported

    echo " === grpc_exported file data start..."
    cat $grpc_exported
    echo " === grpc_exported file data end"
    diff $want_exported $grpc_exported

    kill $METASRV_PID
    sleep 1
}

metactl_import_export 'V003' "$meta_json_v002" "$want_exported_v003" "$want_snapshot_v003"


echo " === "
echo " === 4. Test export from empty running metasrv to file $grpc_exported"
echo " ===    Check important entries"
echo " === "

rm -rf "$meta_dir"

echo " === start a single node databend-meta"
# test export from grpc
./target/${BUILD_PROFILE}/databend-meta --single --raft-dir "$meta_dir" --log-file-level=debug &
METASRV_PID=$!
echo $METASRV_PID
sleep 10

echo " === export data from a running databend-meta to $grpc_exported"
./target/${BUILD_PROFILE}/databend-metactl export --grpc-api-address "localhost:9191" >$grpc_exported

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
if grep -Fxq '["header",{"DataHeader":{"key":"header","value":{"version":"V003","upgrading":null}}}]' $grpc_exported; then
    echo " === Header record found, good!"
else
    echo " === No Header record found!!!"
    exit 1
fi


kill $METASRV_PID

sleep 3

echo " === "
echo " === 5. Test import data with incompatible header $grpc_exported to dir $meta_dir"
echo " === "


echo '["header",{"DataHeader":{"key":"header","value":{"version":"V100","upgrading":null}}}]' > $grpc_exported

echo " === import into $meta_dir"
cat $grpc_exported |
    ./target/${BUILD_PROFILE}/databend-metactl import --raft-dir "$meta_dir"  \
    && { echo " === expect error when importing incompatible header"; exit 1; } \
    || echo " === error is expected. OK";
