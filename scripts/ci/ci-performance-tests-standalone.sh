#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

echo "Starting standalone FuseQuery(release)"
./scripts/deploy/fusequery-standalone.sh release

SCRIPT_PATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_PATH/../../tests/perfs" || exit

echo "Starting fuse perfs"

d_pull="/tmp/perf_${RANDOM}"
d_master="/tmp/perf_${RANDOM}"

mkdir -p "${d_pull}"
mkdir -p "${d_master}"

python perfs.py --output "/tmp/${d_pull}"


./scripts/deploy/fusequery-standalone_lastest.sh release
python perfs.py --output "/tmp/${d_master}"

python compare.py -m "${d_pull}"  -p "${d_master}"