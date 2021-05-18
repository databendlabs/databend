#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_PATH/../.." || exit

killall fuse-query
sleep 1

echo 'Start...'
BIN=${1:-debug}
nohup target/${BIN}/fuse-query -c scripts/deploy/config/fusequery-cluster-1.toml &

echo "Waiting on 10 seconds..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 3307
