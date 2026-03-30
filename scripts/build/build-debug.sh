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

QUERY_FEATURE_PROFILE="${QUERY_FEATURE_PROFILE:-default}"
QUERY_FEATURES="${QUERY_FEATURES:-}"

query_build_args=()

if [[ -n "$QUERY_FEATURES" ]]; then
	query_build_args=(--no-default-features --features "$QUERY_FEATURES")
	echo "Building databend-query with explicit QUERY_FEATURES=$QUERY_FEATURES"
else
	case "$QUERY_FEATURE_PROFILE" in
	default | full) ;;
	lean)
		query_build_args=(--no-default-features --features "simd")
		echo "Building databend-query with QUERY_FEATURE_PROFILE=lean (features: simd)"
		;;
	stage)
		query_build_args=(--no-default-features --features "simd,storage-stage")
		echo "Building databend-query with QUERY_FEATURE_PROFILE=stage (features: simd,storage-stage)"
		;;
	*)
		echo "Unknown QUERY_FEATURE_PROFILE: $QUERY_FEATURE_PROFILE"
		echo "Supported profiles: default, full, lean, stage"
		echo "Or set QUERY_FEATURES explicitly, for example: QUERY_FEATURES=simd,virtual-column"
		exit 1
		;;
	esac
fi

if [[ ${#query_build_args[@]} -eq 0 ]]; then
	cargo build --bin=databend-query --bin=databend-meta --bin=databend-metactl --bin=databend-sqllogictests --bin=databend-sqlsmith
else
	cargo build -p databend-binaries --bin=databend-query "${query_build_args[@]}"
	cargo build -p databend-meta-binaries --bin=databend-meta --bin=databend-metactl
	cargo build -p databend-sqllogictests --bin=databend-sqllogictests
	cargo build -p databend-sqlsmith --bin=databend-sqlsmith
fi

echo "All done..."
