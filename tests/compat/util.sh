#!/bin/bash

# Supporting utilities for compatiblity test.

query_config_path="scripts/ci/deploy/config/databend-query-node-1.toml"
bend_repo_url="https://github.com/datafuselabs/databend"


# Build the url to download specified version of databend binaries.
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



# Download a specific version of databend, untar it to folder `./bins/$ver`
# `ver` is semver without prefix `v` or `-nightly`
download_binary() {
    local ver="$1"
    local required_fn="${2-databend-query}"

    local url="$(binary_url $ver)"
    local fn="databend-$ver.tar.gz"

    echo " === Start to download $fn from $url"

    if [ -f ./bins/$ver/$required_fn ]; then
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
