#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Starting standalone DatabendQuery and DatabendMeta"
./scripts/ci/deploy/databend-query-standalone.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests/fuzz" || exit

# Metamorphic oracle testing: random predicates checked against identities that
# hold for every predicate (TLP / NoREC). Python stdlib only; deterministic per
# seed, and a failure prints the seed and the SQL statements to reproduce.
echo "Starting databend metamorphic oracle tests"
TLP_ITERATIONS=${TLP_ITERATIONS:-400} python3 tlp.py
