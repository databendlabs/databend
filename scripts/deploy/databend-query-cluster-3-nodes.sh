#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

killall databend-query
killall databend-meta
sleep 1

echo 'Start Meta service HA cluster(3 nodes)...'
nohup ./target/debug/databend-meta --single true --id 1 &
python scripts/ci/wait_tcp.py --timeout 5 --port 9191

nohup ./target/debug/databend-meta --id 2 --raft-dir "./_meta2" --metric-api-address 0.0.0.0:29000 --admin-api-address 0.0.0.0:29002 --flight-api-address 0.0.0.0:29003 --log-dir ./_logs2 --raft-api-port 29009 --join 127.0.0.1:28004 &
python scripts/ci/wait_tcp.py --timeout 5 --port 29000

nohup ./target/debug/databend-meta --id 3 --raft-dir "./_meta3" --metric-api-address 0.0.0.0:30000 --admin-api-address 0.0.0.0:30002 --flight-api-address 0.0.0.0:30003 --log-dir ./_logs3 --raft-api-port 30009 --join 127.0.0.1:28004 127.0.0.1:30009 &
python scripts/ci/wait_tcp.py --timeout 5 --port 30000

# not work configure(for now) (error config)

#nohup ./target/debug/databend-meta --single true --id 1  --raft-dir "./_meta2" --metric-api-address 0.0.0.0:29000 --admin-api-address 0.0.0.0:29002 --flight-api-address 0.0.0.0:29003 --log-dir ./_logs2 --raft-api-port 29009 &
#echo "Waiting on databend-meta 10 seconds..."
#python scripts/ci/wait_tcp.py --timeout 5 --port 29000
#
#nohup ./target/debug/databend-meta --id 2  --join 127.0.0.1:29009 &
#python scripts/ci/wait_tcp.py --timeout 5 --port 9191
#
#nohup ./target/debug/databend-meta --id 3 --raft-dir "./_meta3" --metric-api-address 0.0.0.0:30000 --admin-api-address 0.0.0.0:30002 --flight-api-address 0.0.0.0:30003 --log-dir ./_logs3 --raft-api-port 30009 --join 127.0.0.1:28004 127.0.0.1:30009 &
#python scripts/ci/wait_tcp.py --timeout 5 --port 30000

# Situation1: start error config(no raft snapshot), it would fail on stateless tests
# Situation2: start ok config, once it pass, start error config without clean raft data, it would fail during startup
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

echo "All done..."
