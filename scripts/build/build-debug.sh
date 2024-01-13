#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

echo "Build(DEBUG) start..."

if [[ $(uname -a) =~ Darwin ]]; then
	export CMAKE_CC_COMPILER=/opt/homebrew/opt/llvm/bin/clang
	export CMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm/bin/clang++
	export JEMALLOC_SYS_WITH_LG_PAGE=14
	export JEMALLOC_SYS_WITH_MALLOC_CONF=oversize_threshold:0,dirty_decay_ms:5000,muzzy_decay_ms:5000
fi

cargo build --bin=databend-query --bin=databend-meta --bin=databend-metactl --bin=databend-sqllogictests --bin=databend-sqlsmith
echo "All done..."
