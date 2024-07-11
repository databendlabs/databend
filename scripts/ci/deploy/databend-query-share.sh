#!/bin/bash
# Copyright 2023 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.
set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../.." || exit
BUILD_PROFILE=${BUILD_PROFILE:-debug}

killall databend-query || true
killall databend-meta || true
killall share-endpoint || true
sleep 1

echo "build share endpoint"
git clone git@github.com:datafuselabs/share-endpoint.git
cd share-endpoint
cargo build --release


echo "Start query node1 for sharding data"
