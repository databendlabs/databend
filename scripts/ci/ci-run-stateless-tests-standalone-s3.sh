#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

echo "*************************************"
echo "* Setting STORAGE_TYPE to S3.       *"
echo "*                                   *"
echo "* Please make sure that S3 backend  *"
echo "* is ready, and configured properly.*"
echo "*************************************"
export STORAGE_TYPE=s3
export S3_STORAGE_BUCKET=testbucket
export S3_STORAGE_REGION=us-east-1
export S3_STORAGE_ENDPOINT_URL=http://127.0.0.1:9000
export S3_STORAGE_ACCESS_KEY_ID=minioadmin
export S3_STORAGE_SECRET_ACCESS_KEY=minioadmin
echo "calling test suite"
bash ./scripts/ci/ci-run-stateless-tests-standalone.sh

