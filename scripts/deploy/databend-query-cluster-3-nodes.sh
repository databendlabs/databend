#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

killall databend-query
killall databend-store
sleep 1

echo 'Start one DatabendStore...'
nohup target/debug/databend-store  --single=true --log-level=ERROR &
echo "Waiting on databend-store 10 seconds..."
python scripts/ci/wait_tcp.py --timeout 5 --port 9191

echo 'Start DatabendQuery node-1'
nohup target/debug/databend-query -c scripts/deploy/config/databend-query-node-1.toml &

echo "Waiting on node-1..."
python scripts/ci/wait_tcp.py --timeout 5 --port 9091

echo 'Start DatabendQuery node-2'
nohup target/debug/databend-query -c scripts/deploy/config/databend-query-node-2.toml &

echo "Waiting on node-2..."
python scripts/ci/wait_tcp.py --timeout 5 --port 9092

echo 'Start DatabendQuery node-3'
nohup target/debug/databend-query -c scripts/deploy/config/databend-query-node-3.toml &

echo "Waiting on node-3..."
python scripts/ci/wait_tcp.py --timeout 5 --port 9093

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
