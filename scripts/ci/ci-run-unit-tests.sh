#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit

echo "Starting unit tests"
env "MACOSX_DEPLOYMENT_TARGET=10.7" cargo test
