#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

echo "Starting DatabendQuery management mode(debug)"
./scripts/ci/deploy/databend-query-management-mode.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting databend-test for management mode"
./databend-test --mode 'standalone' --run-dir 2_management_mode
