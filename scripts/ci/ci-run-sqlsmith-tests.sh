#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Starting standalone DatabendQuery and DatabendMeta"
./scripts/ci/deploy/databend-query-standalone.sh

BUILD_PROFILE=${BUILD_PROFILE:-debug}

echo 'Starting databend-sqlsmith tests...'
nohup target/${BUILD_PROFILE}/databend-sqlsmith
