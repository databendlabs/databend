#!/bin/bash

set -o errexit
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
echo " === SCRIPT_PATH: $SCRIPT_PATH"
# go to work tree root
cd "$SCRIPT_PATH/../../"
pwd

BUILD_PROFILE="${BUILD_PROFILE:-debug}"

usage() {
    echo " === test databend-meta cluster: join and leave"
    echo " === Expect ./target/${BUILD_PROFILE} contains the binaries"
    echo " === Usage: $0"
}

kill_proc() {
    local name="$1"

    echo " === Kill $name ..."

    killall "$name" || {
        echo " === no "$name" to kill"
        return
    }

    sleep 1

    if test -n "$(pgrep $name)"; then
        echo " === The $name is not killed. force killing."
        killall -9 $name || echo " === no $Name to killall-9"
    fi

    echo " === Done kill $name"
}

# Test specified version of query and meta
run_test() {
    local metasrv="./target/${BUILD_PROFILE}/databend-meta"

    echo " === metasrv version:"
    "$metasrv" --cmd ver

    kill_proc databend-meta

    echo " === Clean old meta dir"
    rm -rf .databend/meta* || echo " === no meta* dir to rm"

    rm nohup.out || echo "no nohup.out"

    export RUST_BACKTRACE=1

    echo ' === Start databend-meta 1...'

    nohup "$metasrv" -c scripts/ci/deploy/config/databend-meta-node-1.toml --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 30 --port 9191

    echo ' === Start databend-meta 2...'

    nohup "$metasrv" -c scripts/ci/deploy/config/databend-meta-node-2.toml --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 30 --port 28202

    echo ' === Start databend-meta 3...'

    nohup "$metasrv" -c scripts/ci/deploy/config/databend-meta-node-3.toml --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 30 --port 28302

    sleep 5
    echo " === node 1 in cluster"
    curl -s localhost:28101/v1/cluster/nodes | grep 28103
    echo " === node 2 in cluster"
    curl -s localhost:28101/v1/cluster/nodes | grep 28203
    echo " === node 3 in cluster"
    curl -s localhost:28101/v1/cluster/nodes | grep 28303

    echo " === leave node 3"

    "$metasrv" --leave-id 3 --leave-via 127.0.0.1:28103

    sleep 5
    echo " === node 3 NOT in cluster"
    { curl -s localhost:28101/v1/cluster/nodes | grep 28303; } && {
        echo "node 3 still in cluster"
        return 1
    } || echo " === Done removing node 3"
}

# -- main --

chmod +x ./target/${BUILD_PROFILE}/*

echo " === current metasrv ver: $(./target/${BUILD_PROFILE}/databend-meta --single --cmd ver | tr '\n' ' ')"

run_test
