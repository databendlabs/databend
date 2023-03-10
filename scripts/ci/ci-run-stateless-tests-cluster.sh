#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Starting Cluster databend-query"

# Enable backtrace to debug https://github.com/datafuselabs/databend/issues/7986
export RUST_BACKTRACE=full
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

for i in `seq 1 3`
do
    echo "Starting databend-test $i"
    # 13_0004_q4: https://github.com/datafuselabs/databend/issues/8107
    ./databend-test --mode 'cluster' --run-dir 0_stateless --skip '13_0004_q4'

    if [ $? -ne 0 ];then
        break
    fi
done
