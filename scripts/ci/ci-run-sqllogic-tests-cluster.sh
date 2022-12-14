#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Starting Cluster databend-query"
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

echo -e "ulimit:\n$(ulimit -a)"

echo "Starting databend-sqllogic tests under mysql"
cargo run -p sqllogictests -- --handler mysql

echo "Starting databend-sqllogic tests under http"
cargo run -p sqllogictests -- --handler http

echo "Starting databend-sqllogic tests under clickhouse"
cargo run -p sqllogictests -- --handler clickhouse