#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Iceberg Rest integration tests"
echo "Starting standalone DatabendQuery(debug profile)"
./scripts/ci/deploy/databend-query-standalone.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting databend-test"
./databend-test --mode 'standalone' --run-dir 3_stateful_iceberg
