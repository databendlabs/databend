#!/bin/sh

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
bend_repo_url="https://github.com/datafuselabs/databend"

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

# Clone only specified dir or file in the specified commit
git_partial_clone() {
    local repo_url="$1"
    local branch="$2"
    local worktree_path="$3"
    local local_path="$4"

    echo " === Clone $repo_url@$branch:$worktree_path"
    echo " ===    To $local_path/$worktree_path"

    rm -rf "$local_path" || echo "no $local_path"

    git clone \
        -b "$branch" \
        --depth 1 \
        --filter=blob:none \
        --sparse \
        "$repo_url" \
        "$local_path"

    cd "$local_path"
    git sparse-checkout set "$worktree_path"

    echo " === Done clone from $repo_url@$branch:$worktree_path"

    ls "$worktree_path"

}


# Run specified tests found in logic test suite dir
run_logictest()
{
    local pattern="$1"

    (
        cd "tests/logictest"
        python3 main.py --suites "$SCRIPT_PATH/compat-logictest" "$pattern"
    )

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
    local metasrv="./bins/current/databend-meta"


    echo " === metasrv version:"
    # TODO remove --single
    "$metasrv" --single --cmd ver || echo " === no version yet"

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

    nohup "$metasrv" --single --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 5 --port 9191


    # Only run test on mysql handler
    export DISABLE_HTTP_LOGIC_TEST=true
    export DISABLE_CLICKHOUSE_LOGIC_TEST=true


    echo ' === Start old databend-query...'

    # (
    #     download_query_config "$query_old_ver" old_config/$query_old_ver
    # )
    # config_path="old_config/$query_old_ver/$query_config_path"
    config_path="./bins/$query_old_ver/configs/databend-query.toml"


    # TODO clean up data?
    echo " === bring up $query_old"

    nohup "$query_old" -c "$config_path" --log-level DEBUG --meta-endpoints "0.0.0.0:9191" >query-old.log &
    python3 scripts/ci/wait_tcp.py --timeout 5 --port 3307

    echo " === Run test: fuse_compat_write with old query"

    # download_logictest $query_old_ver old_logictest/$query_old_ver
    # run_logictest old_logictest/$query_old_ver fuse_compat_write
    run_logictest fuse_compat_write


    kill_proc databend-query


    echo " === bring up new query "

    config_path="$query_config_path"

    nohup "$query_new" -c "$config_path" --log-level DEBUG --meta-endpoints "0.0.0.0:9191" >query-current.log &
    python3 scripts/ci/wait_tcp.py --timeout 5 --port 3307


    echo " === Run test: fuse_compat_read with current query"

    # download_logictest $query_old_ver old_logictest
    run_logictest fuse_compat_read
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
