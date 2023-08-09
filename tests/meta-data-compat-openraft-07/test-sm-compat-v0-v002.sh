#!/bin/sh

usage()
{
    cat <<-END
- Import just raft-log without state machine;
- Start a databend-meta and wait for it to apply raft-logs to state machine;
- Trigger snapshot building and compare if the snapshot of state machine is the same as former version's.
END
    exit 0

}

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"

meta_dir="$SCRIPT_PATH/_meta_dir"
raft_log_json="$SCRIPT_PATH/v0-raft-log.txt"
want_json="$SCRIPT_PATH/want_exported_sm_v002"
exported="$SCRIPT_PATH/exported"

rm -rf "$meta_dir" || echo "no dir to remove: $meta_dir"

chmod +x ./target/${BUILD_PROFILE}/databend-metactl

echo " === import into $meta_dir"
cat $raft_log_json |
    ./target/${BUILD_PROFILE}/databend-metactl --import --raft-dir "$meta_dir"

sleep 1

echo " === start a single node databend-meta"
# test export from grpc
chmod +x ./target/${BUILD_PROFILE}/databend-meta

# Give it a very big heartbeat interval to prevent election.
# Election will change the `vote` in storage and thus fail the following `diff`
# in this test.
./target/${BUILD_PROFILE}/databend-meta --heartbeat-interval 100000 --log-file-level DEBUG --single --id 1 --raft-dir "$meta_dir" &
METASRV_PID=$!
echo "meta-service pid:" $METASRV_PID
sleep 10

echo " === trigger snapshot so that sm will be exported"
curl localhost:28002/v1/ctrl/trigger_snapshot

sleep 3

echo " === export data from a running databend-meta to $exported, filter out non-sm lines"
./target/${BUILD_PROFILE}/databend-metactl --export --grpc-api-address "localhost:9191" | grep 'state_machine/0' >$exported

echo " === exported file data start..."
cat $exported
echo " === exported file data end"

echo " === check backup date $want_json and exported $exported"
diff $want_json $exported

kill $METASRV_PID
