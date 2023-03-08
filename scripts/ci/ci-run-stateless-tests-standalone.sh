#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Starting standalone DatabendQuery(debug)"
./scripts/ci/deploy/databend-query-standalone.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

for i in `seq 1 3`
do
    echo "Starting databend-test $i"
    ./databend-test --mode 'standalone' --run-dir 0_stateless

    if [ $? -ne 0 ];then
        break
    fi
done

