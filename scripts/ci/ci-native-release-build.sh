#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

echo "Starting build native release"
SCRIPT_PATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_PATH/../../" || exit

echo "Starting build fuse-query"
RUST_BACKTRACE=full RUSTFLAGS="-C target-cpu=native" cargo build --bin=fuse-query --release