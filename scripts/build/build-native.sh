#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

echo "Build(NATIVE) start..."
RUSTFLAGS="-C target-cpu=native" cargo build --bin=fuse-query --bin=fuse-benchmark --bin=fuse-store --release
echo "All done..."
