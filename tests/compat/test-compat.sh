#!/bin/sh

set -o errexit

usage()
{
    echo "test query being compatible with old metasrv"
    echo "Expect current release binaries are in ./current/."
    echo "Expect     old release binaries are in ./old/."
    echo "Usage: $0"
}

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"

echo "SCRIPT_PATH: $SCRIPT_PATH"

# go to work tree root
cd "$SCRIPT_PATH/../../"

pwd

mkdir -p ./target/debug/
chmod +x ./current/*
chmod +x ./old/*

cp ./old/databend-query    ./target/debug/
cp ./current/databend-meta ./target/debug/

./target/debug/databend-meta --version
./target/debug/databend-query --version

./scripts/ci/ci-run-stateless-tests-standalone.sh
