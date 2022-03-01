#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

echo "Build(DEBUG) start..."
cargo build --bin=databend-query --bin=databend-benchmark --bin=databend-meta --bin=databend-metactl
echo "All done..."
