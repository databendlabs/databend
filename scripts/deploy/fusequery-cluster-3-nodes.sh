#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_PATH/../.." || exit

killall fuse-query
sleep 1

echo 'start cluster-1'
nohup target/debug/fuse-query -c scripts/ci/config/fusequery-cluster-1.toml &

echo "Waiting on cluster-1..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 9091

echo 'start cluster-2'
nohup target/debug/fuse-query -c scripts/ci/config/fusequery-cluster-2.toml &

echo "Waiting on cluster-2..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 9092

echo 'start cluster-3'
nohup target/debug/fuse-query -c scripts/ci/config/fusequery-cluster-3.toml &

echo "Waiting on cluster-3..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 9093

curl http://127.0.0.1:8081/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster1","address":"0.0.0.0:9091", "priority":3, "cpus":8}'
curl http://127.0.0.1:8081/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster2","address":"0.0.0.0:9092", "priority":3, "cpus":8}'
curl http://127.0.0.1:8081/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster3","address":"0.0.0.0:9093", "priority":1, "cpus":8}'

echo "All done..."

