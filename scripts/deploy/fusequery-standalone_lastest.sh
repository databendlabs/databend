#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.


get_latest_release() {
  curl --silent "https://api.github.com/repos/$1/releases/latest" | # Get latest release from GitHub api
    grep '"tag_name":' |                                            # Get tag line
    sed -E 's/.*"([^"]+)".*/\1/'                                    # Pluck JSON value
}

latest=`get_latest_release "datafuselabs/datafuse"`

SCRIPT_PATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_PATH/../.." || exit

killall fuse-store
killall fuse-query
sleep 1

BIN=${1:-debug}

echo 'Start FuseStore...'
nohup target/${BIN}/fuse-store&
echo "Waiting on fuse-store 10 seconds..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 9191

echo 'Start FuseQuery...'
nohup target/${BIN}/fuse-query -c scripts/deploy/config/fusequery-node-1.toml &
echo "Waiting on fuse-query 10 seconds..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 3307

cp target/${BIN}/fuse-benchmark /usr/bin/
