#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

BASE_DIR=`pwd`
echo "Starting standalone FuseQuery(release)"
${BASE_DIR}/scripts/deploy/fusequery-standalone.sh release

SCRIPT_PATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_PATH/../../tests/perfs" || exit

echo "Starting fuse perfs"

d_pull="/tmp/perf_${RANDOM}"
d_release="/tmp/perf_${RANDOM}"

mkdir -p "${d_pull}"
mkdir -p "${d_release}"

## run perf for current
python perfs.py --output "${d_pull}" --bin "${BASE_DIR}/target/release/fuse-benchmark" --host 127.0.0.1 --port 9001

## run perf for latest release
${BASE_DIR}/scripts/deploy/fusequery-standalone-from-release.sh
python perfs.py --output "${d_release}" --bin "${BASE_DIR}/target/release/fuse-benchmark"  --host 127.0.0.1 --port 9001

## run comparation scripts
python compare.py -r "${d_release}"  -p "${d_pull}"