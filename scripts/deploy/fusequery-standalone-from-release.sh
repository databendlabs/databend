#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

tag=$1

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

killall datafuse-store
killall datafuse-query
sleep 1

wget --quiet -O target/fusequery-${tag}-linux-x86_64 "https://github.com/datafuselabs/datafuse/releases/download/${tag}/fusequery-${tag}-linux-x86_64"
chmod +x target/fusequery-${tag}-linux-x86_64

echo 'Start FuseQuery...'
nohup ./target/fusequery-${tag}-linux-x86_64 -c scripts/deploy/config/fusequery-node-1.toml &
echo "Waiting on datafuse-query 10 seconds..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 3307
