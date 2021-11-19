#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

echo "EXPORTING TLS RPC CONFIGURATION ENV VARS"
export RPC_TLS_SERVER_CERT="./tests/certs/server.pem";
export RPC_TLS_SERVER_KEY="./tests/certs/server.key";
export RPC_TLS_QUERY_SERVER_ROOT_CA_CERT="./tests/certs/ca.pem";
export RPC_TLS_QUERY_SERVICE_DOMAIN_NAME="localhost";
export RPC_TLS_STORE_SERVER_ROOT_CA_CERT="./tests/certs/ca.pem";
export RPC_TLS_STORE_SERVICE_DOMAIN_NAME="localhost";

echo "calling test suite"
./scripts/ci/ci-run-stateless-tests-cluster.sh
