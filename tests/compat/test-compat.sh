#!/bin/sh

set -o errexit
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
echo " === SCRIPT_PATH: $SCRIPT_PATH"
# go to work tree root
cd "$SCRIPT_PATH/../../"
pwd

usage()
{
    echo " === test latest query being compatible with minimal compatible metasrv"
    echo " === test latest metasrv being compatible with minimal compatible query"
    echo " === Expect ./bins/current contains current binaries"
    echo " === Usage: $0"
}

binary_url()
{
    local ver="$1"
    echo "https://github.com/datafuselabs/databend/releases/download/v${ver}-nightly/databend-v${ver}-nightly-x86_64-unknown-linux-gnu.tar.gz"
}

# output: 0.7.58
# Without prefix `v` and `-nightly`
find_min_query_ver()
{
    ./bins/current/databend-meta --single --cmd ver | grep min-compatible-client-version |  awk '{print $2}'
}

find_min_metasrv_ver()
{
    ./bins/current/databend-query --cmd ver | grep min-compatible-metasrv-version |  awk '{print $2}'
}

# download a specific version of databend, untar it to folder `./bins/$ver`
# `ver` is semver without prefix `v` or `-nightly`
download_binary()
{
    local ver="$1"

    local url="$(binary_url $ver)"
    local fn="databend-$ver.tar.gz"

    if [ -f ./bins/$ver/databend-meta ]; then
        echo " === binaries exist: $(ls ./bins/$ver/* | tr '\n' ' ')"
        chmod +x ./bins/$ver/*
        return
    fi

    if [ -f "$fn" ]; then
        echo " === tar file exists: $fn"
    else
        echo " === Download binary ver: $ver"
        echo " === Download binary url: $url"

        wget -q "$url" -o "$fn"
        # or:
        # curl -L "$url" > "$fn"
    fi

    mkdir -p ./bins/$ver
    tar -xf "$fn" -C ./bins/$ver

    chmod +x ./bins/$ver/*
}


# Download test suite for a specific version of query.
download_test_suite()
{
    local ver="$1"

    local path="tests/suites/0_stateless/05_ddl"

    echo " === Download test suites from $ver:$path"

    rm -rf shadow || echo "no shadow dir"

    git clone \
        -b v$ver-nightly \
        --depth 1  \
        --filter=blob:none  \
        --sparse \
        "https://github.com/datafuselabs/databend" \
        shadow

    cd shadow
    git sparse-checkout set $path

    echo " === Done download test suites from $ver:$path"

    ls $path
}

kill_proc()
{
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
run_test()
{
    local query_ver="$1"
    local metasrv_ver="$2"

    echo " === Test with query-$query_ver and metasrv-$metasrv_ver"

    local query="./bins/$query_ver/databend-query"
    local metasrv="./bins/$metasrv_ver/databend-meta"

    # "$metasrv" --single --cmd ver

    echo " === metasrv version:"
    # TODO remove --single
    "$metasrv" --single --cmd ver || echo " === no version yet"

    echo " === query version:"
    "$query" --cmd ver || echo " === no version yet"

    sleep 1

    kill_proc databend-query
    kill_proc databend-meta

    echo " === Clean old meta dir"
    rm -rf .databend/meta || echo " === no meta dir to rm"

    rm nohup.out

    export RUST_BACKTRACE=1

    echo ' === Start databend-meta...'
    nohup "$metasrv" --single --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 5 --port 9191

    echo ' === Start databend-query...'
    nohup "$query" -c scripts/ci/deploy/config/databend-query-node-1.toml &
    python3 scripts/ci/wait_tcp.py --timeout 5 --port 3307

    echo " === Starting metasrv related test: 05_ddl"
    if [ "$query_ver" = "current" ]; then
        ./tests/databend-test --suites tests/suites --mode 'standalone' --run-dir 0_stateless -- 05_
    else
        (
            # download suites into ./shadow
            download_test_suite $query_ver;
        )
        ./tests/databend-test --suites shadow/tests/suites --mode 'standalone' --run-dir 0_stateless -- 05_
    fi
}

# -- main --

echo " === current metasrv ver: $(./bins/current/databend-meta --single --cmd ver | tr '\n' ' ')"
echo " === current   query ver: $(./bins/current/databend-query --cmd ver | tr '\n' ' ')"

old_query_ver=$(find_min_query_ver)
old_metasrv_ver=$(find_min_metasrv_ver)

echo " === min query ver: $old_query_ver"
echo " === min metasrv ver: $old_metasrv_ver"

download_binary "$old_metasrv_ver"
download_binary "$old_query_ver"

mkdir -p ./target/debug/

run_test current        current
run_test current        $old_metasrv_ver
run_test $old_query_ver current
