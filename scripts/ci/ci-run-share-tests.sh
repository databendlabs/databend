#!/bin/bash
# Copyright 2020-2023 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Install dependence"
python3 -m pip install --quiet mysql-connector-python
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"

./scripts/ci/deploy/databend-query-share.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

./databend-test $1 --mode 'standalone' --run-dir 3_share_integration --print-time --timeout 120
