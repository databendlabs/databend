#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit

echo "Starting unit tests"
env "MACOSX_DEPLOYMENT_TARGET=10.13" "RUST_TEST_THREADS=2" cargo nextest run

# Do not add leading space as indent to below flags, it will cause `cargo
# valgrind` to fail with `valgrind: command not found` error.
export VALGRINDFLAGS="--show-leak-kinds=definite \
--errors-for-leak-kinds=definite \
--max-threads=1024"

if [[ "$(uname -s)" == "Linux" ]] && [[ "$(uname -m)" == "x86_64" ]]; then
    # If your test case is failing due to memory leak check, you may
    # mark test case as skip-memcheck in `.config/nextest.toml` file
    env "MACOSX_DEPLOYMENT_TARGET=10.13" "RUST_TEST_THREADS=2" \
        cargo valgrind nextest run -E 'not group(skip-memcheck)' \
        --profile memcheck
fi
