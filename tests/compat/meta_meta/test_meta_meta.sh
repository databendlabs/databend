#!/bin/bash

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
echo " === SCRIPT_PATH: $SCRIPT_PATH"
# go to work tree root
cd "$SCRIPT_PATH/../../../"
ROOT="$(pwd)"
pwd

export RUST_BACKTRACE=full

BUILD_PROFILE="${BUILD_PROFILE:-debug}"

source "${SCRIPT_PATH}/../util.sh"

usage() {
    echo " === Assert that latest databend-meta being compatible with an old version databend-meta"
    echo " === Expect ./bins/current contains current version binaries"
    echo " === Usage: $0 <leader-meta-ver> <follower-meta-ver>"
}

admin_addr() {
  echo "127.0.0.1:1000$1"
}

grpc_addr() {
  echo "127.0.0.1:1100$1"
}

raft_addr() {
  echo "127.0.0.1:1200$1"
}

# $0 bring_up_databend_meta $ver $id ...
# The other args are passed to databend-meta
bring_up_databend_meta() {
  local ver="$1"
  local id="$2"
  shift
  shift

  ./bins/$ver/bin/databend-meta \
    --id "$id" \
    --admin-api-address 127.0.0.1:1000$id \
    --grpc-api-address 127.0.0.1:1100$id \
    \
    --raft-listen-host 127.0.0.1 \
    --raft-advertise-host 127.0.0.1 \
    --raft-api-port 1200$id \
    --raft-dir ./.databend/meta_$id/ \
    \
    --max-applied-log-to-keep 0 \
    \
    --log-dir ./.databend/meta_log_$id/ \
    --log-stderr-on \
    --log-stderr-level WARN \
    --log-stderr-format text \
    --log-file-on \
    --log-file-level DEBUG \
    --log-file-format text \
    \
    "$@" &

  python3 scripts/ci/wait_tcp.py --timeout 20 --port 1100$id

  echo " === OK: databend-meta ver=$ver id=$id started"

}


# -- main --

# The meta leader version to assert compatibility with
# e.g. leader_meta_ver="0.7.151"
leader_meta_ver="$1"

# The meta follower version runs with leader_meta_ver
follower_meta_ver="$2"

chmod +x ./bins/current/bin/*

echo " === leader_meta_ver : ${leader_meta_ver}"
echo " === follower_meta_ver : ${follower_meta_ver}"
echo " === current meta ver: $(./bins/current/bin/databend-meta --single --cmd ver | tr '\n' ' ')"

if [ ".$follower_meta_ver" != ".current" ]; then
  download_binary "$follower_meta_ver" databend-meta
fi
if [ ".$leader_meta_ver" != ".current" ]; then
  download_binary "$leader_meta_ver" databend-meta
fi

kill_proc databend-meta

rm -rf ./.databend || echo " === No .databend folder found, skip"

echo " === Bring up leader meta service, ver: $leader_meta_ver"
bring_up_databend_meta "$leader_meta_ver" "1" --single

echo " === Feed data to leader"
./bins/current/bin/databend-metabench \
    --rpc 'table_copy_file:{"file_cnt":5,"ttl_ms":86400999}' \
    --client 1 \
    --number 100 \
    --prefix "1" \
    --grpc-api-address $(grpc_addr 1) \
    > /dev/null

echo " === Trigger snapshot on leader"
curl -qs $(admin_addr 1)/v1/ctrl/trigger_snapshot
sleep 3

echo " === Leader status should contains snapshot state"
curl -qs $(admin_addr 1)/v1/cluster/status

echo " === Feed more data to leader"
./bins/current/bin/databend-metabench \
    --rpc 'table_copy_file:{"file_cnt":5,"ttl_ms":86400999}' \
    --client 1 \
    --number 100 \
    --prefix "1" \
    --grpc-api-address $(grpc_addr 1) \
    > /dev/null

echo " === Bring up follower meta service, ver: $follower_meta_ver"
bring_up_databend_meta "$follower_meta_ver" "2" --join "$(raft_addr 1)"

sleep 3

echo " === Follower status should contains snapshot state"
curl -qs $(admin_addr 2)/v1/cluster/status

echo " === Check consistency between leader and follower"
echo ""

echo " === Export leader meta data to ./.databend/leader-tmp"
./bins/$leader_meta_ver/bin/databend-metactl \
    --export \
    --grpc-api-address $(grpc_addr 1) \
    > ./.databend/leader-tmp

echo " === Export follower meta data to ./.databend/follower-tmp"
./bins/$follower_meta_ver/bin/databend-metactl \
    --export \
    --grpc-api-address $(grpc_addr 2) \
    > ./.databend/follower-tmp

echo " === Shutdown databend-meta servers"
killall databend-meta
sleep 3

# Old version SM exported data contains DataHeader

# remove `proposed_at_ms`, which is added 2025-09-30
cat ./.databend/leader-tmp   | grep 'state_machine' | grep -v DataHeader | awk '{gsub(/,"proposed_at_ms":[0-9]+/, ""); print}' | sort > ./.databend/leader-sm
cat ./.databend/follower-tmp | grep 'state_machine' | grep -v DataHeader | awk '{gsub(/,"proposed_at_ms":[0-9]+/, ""); print}' | sort > ./.databend/follower-sm

echo " === diff SM data between Leader and Follower"
diff ./.databend/leader-sm ./.databend/follower-sm



echo " === mkdir to import with latest datbend-metactl"
mkdir -p ./.databend/_upgrade_meta_1
mkdir -p ./.databend/_upgrade_meta_2


# Exported log data format has changed, re-import them and compare.
#
# SM data in V002 does not output in correct order: exp- is after kv-,
# which is out of order when import to rotbl.
#
# Thus we skip all state machine data, but keeps log data and SM meta.

echo " === Import Leader's log data"
cat ./.databend/leader-tmp \
    | grep -v '"Expire":\|"GenericKV":' \
    | ./bins/current/bin/databend-metactl --import --raft-dir ./.databend/_upgrade_meta_1

echo " === Import Follower's log data"
cat ./.databend/follower-tmp \
    | grep -v '"Expire":\|"GenericKV":' \
    | ./bins/current/bin/databend-metactl --import --raft-dir ./.databend/_upgrade_meta_2

# skip DataHeader that contains distinguished version info
# skip NodeId
# sort because newer version export `Sequence` in different order

echo " === Export Leader's data"
./bins/current/bin/databend-metactl --export --raft-dir ./.databend/_upgrade_meta_1 \
    | grep -v 'NodeId\|DataHeader' \
    | sort \
    > ./.databend/leader

echo " === Export Follower's data"
./bins/current/bin/databend-metactl --export --raft-dir ./.databend/_upgrade_meta_2 \
    | grep -v 'NodeId\|DataHeader' \
    | sort \
    > ./.databend/follower


echo " === diff leader exported and follower exported"
diff ./.databend/leader ./.databend/follower
