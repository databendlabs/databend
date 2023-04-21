#!/bin/bash

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
    local logictest_path="tests/fuse-compat/compat-logictest/$2"

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

    config_path="scripts/ci/deploy/config/databend-query-node-1.toml"

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

    echo "new databend config path: $config_path"

    nohup "$query_new" -c "$config_path" --log-level DEBUG --meta-endpoints "0.0.0.0:9191" >query-current.log &
    python3 scripts/ci/wait_tcp.py --timeout 15 --port 3307

    echo " === Run test: fuse_compat_read with current query"

    # download_logictest $query_old_ver old_logictest
    $sqllogictests --handlers mysql --suites "$logictest_path" --run_file fuse_compat_read
}

# Run suppelmentary stateless tests
run_stateless() {
    local case_path="$1"
    local databend_test="tests/databend-test"
    ${databend_test} --mode 'standalone' --suites tests/fuse-compat/compat-stateless --run-dir $case_path
}