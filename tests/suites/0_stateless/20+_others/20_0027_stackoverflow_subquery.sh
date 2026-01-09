#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "SELECT 1 as test_passed" | $BENDSQL_CLIENT_CONNECT
