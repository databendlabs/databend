#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

# Run this test with insecure disabled so external S3 stages don't load credentials
# from the environment and can access public buckets via unsigned requests.
export STORAGE_ALLOW_INSECURE=false

echo "Starting standalone DatabendQuery and DatabendMeta"
./scripts/ci/deploy/databend-query-standalone.sh

TEST_HANDLERS=${TEST_HANDLERS:-"http"}
BUILD_PROFILE=${BUILD_PROFILE:-debug}

echo "Starting databend-sqllogic tests"
target/${BUILD_PROFILE}/databend-sqllogictests \
  --handlers ${TEST_HANDLERS} \
  --run_file tests/sqllogictests/suites/base/05_ddl/05_0016_ddl_stage_public_s3_list.test \
  --enable_sandbox \
  --parallel 1 \
  ${TEST_EXT_ARGS}

