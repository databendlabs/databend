#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -ex

echo "start databend-metaverifier"
echo "START" > /tmp/meta-verifier
# wait 2 second for chao-meta.py
sleep 4
/databend-metaverifier --client ${CLIENT} --time 1800 --remove-percent 10 --number ${NUMBER} --grpc-api-address ${GRPC_ADDRESS} && echo "END" > /tmp/meta-verifier && sleep 30
