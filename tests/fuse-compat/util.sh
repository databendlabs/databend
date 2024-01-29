#!/bin/bash

query_config_path="scripts/ci/deploy/config/databend-query-node-1.toml"
query_test_path="tests/sqllogictests"
bend_repo_url="https://github.com/datafuselabs/databend"


binary_url() {
    local ver="$1"
    echo "https://github.com/datafuselabs/databend/releases/download/v${ver}-nightly/databend-v${ver}-nightly-x86_64-unknown-linux-gnu.tar.gz"
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
		--quiet \
		--filter=blob:none \
		--sparse \
		"$repo_url" \
		"$local_path"

	cd "$local_path"
	git sparse-checkout set "$worktree_path"

	echo " === Done clone from $repo_url@$branch:$worktree_path"

	ls "$worktree_path"

    cd -
}

# Download config.toml for a specific version of query.
download_query_config() {
	local ver="$1"
	local local_dir="$2"

	config_dir="$(dirname $query_config_path)"
	echo " === Download query config.toml from $ver:$config_dir"

	git_partial_clone "$bend_repo_url" "v$ver-nightly" "$config_dir" "$local_dir"
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

        curl --connect-timeout 5 --retry-all-errors --retry 5 --retry-delay 1 -L "$url" -o "$fn"
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
    local old_config_path="$2"
    local logictest_path="tests/fuse-compat/compat-logictest/$3"

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

    # TODO clean up data?
    echo " === bring up $query_old"

    nohup "$query_old" -c "$old_config_path" --log-level DEBUG --meta-endpoints "0.0.0.0:9191" >query-old.log &
    python3 scripts/ci/wait_tcp.py --timeout 15 --port 3307

    echo " === Run test: fuse_compat_write with old query"

    # download_logictest $query_old_ver old_logictest/$query_old_ver
    # run_logictest old_logictest/$query_old_ver fuse_compat_write
    $sqllogictests --handlers mysql --suites "$logictest_path" --run_file fuse_compat_write

    kill_proc databend-query
    kill_proc databend-meta

    echo ' === Start new databend-meta...'

    nohup "$metasrv_new" --single --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 20 --port 9191

    echo " === Start new databend-query..."

    config_path="scripts/ci/deploy/config/databend-query-node-1.toml"
    echo "new databend config path: $config_path"

    nohup "$query_new" -c "$config_path" --log-level DEBUG --meta-endpoints "0.0.0.0:9191" >query-current.log &
    python3 scripts/ci/wait_tcp.py --timeout 30 --port 3307

    echo " === Run test: fuse_compat_read with current query"

    # download_logictest $query_old_ver old_logictest
    $sqllogictests --handlers mysql --suites "$logictest_path" --run_file fuse_compat_read
}

# Test fuse-data forward compatibility between an old version query and the current
# version query.
run_forward_test() {
    echo " === pip list"
    python3 -m pip list

    local query_old_ver="$1"
    local old_config_path="$2"
    local logictest_path="tests/fuse-compat/compat-logictest/$3"

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

    echo ' === Start new databend-meta...'

    nohup "$metasrv_new" --single --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 20 --port 9191

    echo " === Start new databend-query..."

    config_path="scripts/ci/deploy/config/databend-query-node-1.toml"
    echo "new databend config path: $config_path"

    nohup "$query_new" -c "$config_path" --log-level DEBUG --meta-endpoints "0.0.0.0:9191" >query-current.log &
    python3 scripts/ci/wait_tcp.py --timeout 30 --port 3307

    echo " === Run test: fuse_compat_write with current query"

    # download_logictest $query_old_ver old_logictest
    $sqllogictests --handlers mysql --suites "$logictest_path" --run_file fuse_compat_write

    echo ' === Start old databend-meta...'

    nohup "$metasrv_old" --single --log-level=DEBUG &
    python3 scripts/ci/wait_tcp.py --timeout 10 --port 9191

    echo ' === Start old databend-query...'

    # TODO clean up data?
    echo " === bring up $query_old"

    nohup "$query_old" -c "$old_config_path" --log-level DEBUG --meta-endpoints "0.0.0.0:9191" >query-old.log &
    python3 scripts/ci/wait_tcp.py --timeout 15 --port 3307

    echo " === Run test: fuse_compat_read with old query"

    # download_logictest $query_old_ver old_logictest/$query_old_ver
    # run_logictest old_logictest/$query_old_ver fuse_compat_read
    $sqllogictests --handlers mysql --suites "$logictest_path" --run_file fuse_compat_read

    kill_proc databend-query
    kill_proc databend-meta
}

# Run suppelmentary stateless tests
run_stateless() {
    local case_path="$1"
    local databend_test="tests/databend-test"
    ${databend_test} --mode 'standalone' --suites tests/fuse-compat/compat-stateless --run-dir $case_path
}
