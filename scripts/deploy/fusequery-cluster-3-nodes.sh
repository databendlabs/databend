#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_PATH/../.." || exit

killall fuse-query
killall fuse-store
sleep 1

echo 'Start one FuseStore...'
nohup target/debug/fuse-store&
echo "Waiting on fuse-store 10 seconds..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 9191

echo 'Start FuseQuery node-1'
nohup target/debug/fuse-query -c scripts/deploy/config/fusequery-node-1.toml &

echo "Waiting on node-1..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 9091

echo 'Start FuseQuery node-2'
nohup target/debug/fuse-query -c scripts/deploy/config/fusequery-node-2.toml &

echo "Waiting on node-2..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 9092

echo 'Start FuseQuery node-3'
nohup target/debug/fuse-query -c scripts/deploy/config/fusequery-node-3.toml &

echo "Waiting on node-3..."
timeout 10 sh -c 'until nc -z $0 $1; do sleep 1; done' 0.0.0.0 9093

curl http://127.0.0.1:8081/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster1","address":"127.0.0.1:9091", "priority":3, "cpus":8}'
curl http://127.0.0.1:8081/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster2","address":"127.0.0.1:9092", "priority":3, "cpus":8}'
curl http://127.0.0.1:8081/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster3","address":"127.0.0.1:9093", "priority":1, "cpus":8}'

curl http://127.0.0.1:8082/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster1","address":"127.0.0.1:9091", "priority":3, "cpus":8}'
curl http://127.0.0.1:8082/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster2","address":"127.0.0.1:9092", "priority":3, "cpus":8}'
curl http://127.0.0.1:8082/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster3","address":"127.0.0.1:9093", "priority":1, "cpus":8}'

curl http://127.0.0.1:8083/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster1","address":"127.0.0.1:9091", "priority":3, "cpus":8}'
curl http://127.0.0.1:8083/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster2","address":"127.0.0.1:9092", "priority":3, "cpus":8}'
curl http://127.0.0.1:8083/v1/cluster/add -X POST -H "Content-Type: application/json" -d '{"name":"cluster3","address":"127.0.0.1:9093", "priority":1, "cpus":8}'

echo "All done..."

