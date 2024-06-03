#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -ex

echo "start databend-metaverifier with params client:${CLIENT}, number:${NUMBER}, grpc-api-address:${GRPC_ADDRESS}"
echo "START" > /tmp/meta-verifier
# wait for chao-meta.py
sleep 3
/databend-metaverifier --client ${CLIENT} --time 1800 --remove-percent 10 --number ${NUMBER} --grpc-api-address ${GRPC_ADDRESS} && echo "END" > /tmp/meta-verifier

sleep 40
