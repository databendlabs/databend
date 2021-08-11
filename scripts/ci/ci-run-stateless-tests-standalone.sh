#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

echo "Starting standalone DatafuseQuery(debug)"
./scripts/deploy/datafuse-query-standalone.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting datafuse-test"
./datafuse-test --mode 'standalone'
