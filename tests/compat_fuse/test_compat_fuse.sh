#!/bin/bash

set -o errexit

echo "$0: " '==' "$@" '=='

python3 -m pip install argparse

export RUST_BACKTRACE=full

chmod +x ./bins/current/*

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
echo " === SCRIPT_PATH: $SCRIPT_PATH"
# go to work tree root
cd "$SCRIPT_PATH/../../"
pwd

mkdir -p .databend

python3 "$SCRIPT_PATH/test_compat_fuse.py" "$@"
