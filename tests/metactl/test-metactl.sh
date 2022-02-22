#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"

meta_dir="$SCRIPT_PATH/_meta_dir"
meta_json="$SCRIPT_PATH/meta.json"
exported="$SCRIPT_PATH/exported"

chmod +x ./target/debug/databend-metactl

cat $meta_json \
    | ./target/debug/databend-metactl --import --raft-dir "$meta_dir"

./target/debug/databend-metactl --export --raft-dir "$meta_dir" > $exported

diff $meta_json $exported
