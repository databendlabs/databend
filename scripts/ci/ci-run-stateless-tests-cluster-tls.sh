#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

echo "EXPORTING TLS RPC CONFIGURATION ENV VARS"
set -x
export RPC_TLS_SERVER_CERT="./tests/data/certs/server.pem";
export RPC_TLS_SERVER_KEY="./tests/data/certs/server.key";
export RPC_TLS_QUERY_SERVER_ROOT_CA_CERT="./tests/data/certs/ca.pem";
export RPC_TLS_QUERY_SERVICE_DOMAIN_NAME="localhost";
export RPC_TLS_STORE_SERVER_ROOT_CA_CERT="./tests/data/certs/ca.pem";
export RPC_TLS_STORE_SERVICE_DOMAIN_NAME="localhost";
set +x

echo "calling test suite"
./scripts/ci/ci-run-stateless-tests-cluster.sh
