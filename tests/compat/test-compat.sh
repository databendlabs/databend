#!/bin/sh

set -o errexit
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
echo " === SCRIPT_PATH: $SCRIPT_PATH"
# go to work tree root
cd "$SCRIPT_PATH/../../"
pwd

query_config_path="scripts/ci/deploy/config/databend-query-node-1.toml"
query_test_path="tests/suites/0_stateless/05_ddl"
bend_repo_url="https://github.com/datafuselabs/databend"

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

        curl -L "$url" > "$fn"
        # or:
        # wget -q "$url" -o "$fn"
    fi

    mkdir -p ./bins/$ver
    tar -xf "$fn" -C ./bins/$ver

    chmod +x ./bins/$ver/*
}

# Clone only specified dir or file in the specified commit
git_partial_clone()
{
    local repo_url="$1"
    local branch="$2"
    local worktree_path="$3"
    local local_path="$4"

    echo " === Clone $repo_url@$branch:$worktree_path"
    echo " ===    To $local_path/$worktree_path"

    rm -rf "$local_path" || echo "no $local_path"

    git clone \
        -b "$branch" \
        --depth 1  \
        --filter=blob:none  \
        --sparse \
        "$repo_url" \
        "$local_path"

    cd "$local_path"
    git sparse-checkout set "$worktree_path"

    echo " === Done clone from $repo_url@$branch:$worktree_path"

    ls "$worktree_path"

}


# Download test suite for a specific version of query.
download_test_suite()
{
    local ver="$1"

    echo " === Download test suites from $ver:$query_test_path"

    git_partial_clone "$bend_repo_url" "v$ver-nightly" "$query_test_path" old_suite
}

# Download config.toml for a specific version of query.
download_query_config()
{
    local ver="$1"
    local local_dir="$2"

    echo " === Download query config.toml from $ver:$query_config_path"

    git_partial_clone "$bend_repo_url" "v$ver-nightly" "$query_config_path" "$local_dir"
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

    rm nohup.out || echo "no nohup.out"

    export RUST_BACKTRACE=1

    echo ' === Start databend-meta...'

    nohup "$metasrv" --single --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 5 --port 9191

    echo ' === Start databend-query...'

    if [ "$query_ver" = "current" ]; then
        config_path="$query_config_path"
    else
        (
            download_query_config "$query_ver" old_config;
        )
        config_path="old_config/$query_config_path"
    fi

    nohup "$query" -c "$config_path" --log-level DEBUG --meta-endpoints "0.0.0.0:9191" >query.log &
    python3 scripts/ci/wait_tcp.py --timeout 5 --port 3307

    echo " === Run metasrv related test: 05_ddl"

    if [ "$query_ver" = "current" ]; then
        suite_path="tests/suites"
    else
        (
            # download suites into ./old_suite
            download_test_suite $query_ver;
        )
        suite_path="old_suite/tests/suites"
    fi
    ./tests/databend-test --suites "$suite_path" --mode 'standalone' --run-dir 0_stateless -- 05_
}

# -- main --

chmod +x ./bins/current/*

echo " === current metasrv ver: $(./bins/current/databend-meta --single --cmd ver | tr '\n' ' ')"
echo " === current   query ver: $(./bins/current/databend-query --cmd ver | tr '\n' ' ')"

old_query_ver=$(find_min_query_ver)
old_metasrv_ver=$(find_min_metasrv_ver)

echo " === min query ver: $old_query_ver"
echo " === min metasrv ver: $old_metasrv_ver"

download_binary "$old_metasrv_ver"
download_binary "$old_query_ver"

mkdir -p ./target/debug/

run_test current        $old_metasrv_ver
run_test $old_query_ver current
