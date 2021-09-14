#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

get_latest_tag() {
  curl --silent "https://api.github.com/repos/$1/tags" | # Get latest release from GitHub api
    grep '"name":' | # Get tag line
    sed -E 's/.*"([^"]+)".*/\1/' | grep 'v' | head -1
}

tag=$(get_latest_tag "datafuselabs/datafuse")

BASE_DIR=$(pwd)
echo "Starting standalone DatabendQuery(release)"
${BASE_DIR}/scripts/deploy/databend-query-standalone.sh release

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests/perfs" || exit

echo "Starting fuse perfs"

d_pull="/tmp/perf_${RANDOM}"
d_release="/tmp/perf_${tag}"

mkdir -p "${d_pull}"

python3 -m pip install coscmd PyYAML
## run perf for current
python perfs.py --output "${d_pull}" --bin "${BASE_DIR}/target/release/databend-benchmark" --host 127.0.0.1 --port 9001

## run perf for latest release
if [ ! -d "${d_release}" ]; then
  mkdir -p "${d_release}"
  ${BASE_DIR}/scripts/deploy/databend-query-standalone-from-release.sh "${tag}"
  python perfs.py --output "${d_release}" --bin "${BASE_DIR}/target/release/databend-benchmark" --host 127.0.0.1 --port 9001
fi

## run comparation scripts
python compare.py -r "${d_release}" -p "${d_pull}"
