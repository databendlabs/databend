#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting Standalone fuse-query"
../scripts/deploy/fusequery-standalone.sh
echo "Starting fuse-test"
./fuse-test --skip-dir '1_performance'