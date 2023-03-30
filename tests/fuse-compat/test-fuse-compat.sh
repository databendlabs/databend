#!/bin/bash

set -o errexit
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
echo " === SCRIPT_PATH: $SCRIPT_PATH"
# go to work tree root
cd "$SCRIPT_PATH/../../"
ROOT="$(pwd)"
pwd

BUILD_PROFILE="${BUILD_PROFILE:-debug}"

query_config_path="scripts/ci/deploy/config/databend-query-node-1.toml"
logictest_path="tests/fuse-compat/compat-logictest"

usage() {
    echo " === Assert that latest query being compatible with an old version query on fuse-table format"
    echo " === Expect ./bins/current contains current version binaries"
    echo " === Usage: $0 <old_version>"
}

binary_url() {
    local ver="$1"
    echo "https://github.com/datafuselabs/databend/releases/download/v${ver}-nightly/databend-v${ver}-nightly-x86_64-unknown-linux-gnu.tar.gz"
}

# download a specific version of databend, untar it to folder `./bins/$ver`
# `ver` is semver without prefix `v` or `-nightly`
download_binary() {
    local ver="$1"

    local url="$(binary_url $ver)"
    local fn="databend-$ver.tar.gz"

    if [ -f ./bins/$ver/databend-query ]; then
        echo " === binaries exist: $(ls ./bins/$ver/* | tr '\n' ' ')"
        chmod +x ./bins/$ver/*
        return
    fi

    if [ -f "$fn" ]; then
        echo " === tar file exists: $fn"
    else
        echo " === Download binary ver: $ver"
        echo " === Download binary url: $url"

        curl -L "$url" >"$fn"
        # or:
        # wget -q "$url" -o "$fn"
    fi

    mkdir -p ./bins/$ver
    tar -xf "$fn" -C ./bins/$ver

    echo " === unpacked: ./bins/$ver:"
    ls ./bins/$ver

    chmod +x ./bins/$ver/*
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

# Test fuse-data compatibility between an old version query and the current
# version query.
run_test() {
    echo " === pip list"
    python3 -m pip list

    local query_old_ver="$1"

    echo " === Test with query-$query_old_ver and current query"

    local query_old="./bins/$query_old_ver/bin/databend-query"
    local query_new="./bins/current/databend-query"
    local metasrv_old="./bins/$query_old_ver/bin/databend-meta"
    local metasrv_new="./bins/current/databend-meta"
    local sqllogictests="./bins/current/databend-sqllogictests"

    echo " === metasrv version:"
    # TODO remove --single
    "$metasrv_new" --single --cmd ver || echo " === no version yet"

    echo " === old query version:"
    "$query_old" --cmd ver || echo " === no version yet"

    echo " === new query version:"
    "$query_new" --cmd ver || echo " === no version yet"

    sleep 1

    kill_proc databend-query
    kill_proc databend-meta

    echo " === Clean meta dir"
    rm -rf .databend || echo " === no dir to rm: .databend"
    rm nohup.out || echo "no nohup.out"

    echo ' === Start databend-meta...'

    export RUST_BACKTRACE=1

    echo ' === Start old databend-meta...'

    nohup "$metasrv_old" --single --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 10 --port 9191

    echo ' === Start old databend-query...'

    config_path="./bins/$query_old_ver/configs/databend-query.toml"

    # TODO clean up data?
    echo " === bring up $query_old"

    nohup "$query_old" -c "$config_path" --log-level DEBUG --meta-endpoints "0.0.0.0:9191" >query-old.log &
    python3 scripts/ci/wait_tcp.py --timeout 5 --port 3307

    echo " === Run test: fuse_compat_write with old query"

    # download_logictest $query_old_ver old_logictest/$query_old_ver
    # run_logictest old_logictest/$query_old_ver fuse_compat_write
    $sqllogictests --handlers mysql --suites "$logictest_path" --run_file fuse_compat_write

    kill_proc databend-query
    kill_proc databend-meta

    echo ' === Start new databend-meta...'

    nohup "$metasrv_new" --single --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 10 --port 9191

    echo " === Start new databend-query..."

    config_path="$query_config_path"

    nohup "$query_new" -c "$config_path" --log-level DEBUG --meta-endpoints "0.0.0.0:9191" >query-current.log &
    python3 scripts/ci/wait_tcp.py --timeout 15 --port 3307

    echo " === Run test: fuse_compat_read with current query"

    # download_logictest $query_old_ver old_logictest
    $sqllogictests --handlers mysql --suites "$logictest_path" --run_file fuse_compat_read
}

# -- main --

# The previous version to assert compatibility with
# e.g. old_query_ver="0.7.151"
old_query_ver="$1"

chmod +x ./bins/current/*

echo " === current metasrv ver: $(./bins/current/databend-meta --single --cmd ver | tr '\n' ' ')"
echo " === current   query ver: $(./bins/current/databend-query --cmd ver | tr '\n' ' ')"
echo " === old query ver: $old_query_ver"

download_binary "$old_query_ver"

mkdir -p ./target/${BUILD_PROFILE}/

run_test $old_query_ver
