#!/bin/bash
# Copyright 2022 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../.." || exit
BUILD_PROFILE=${BUILD_PROFILE:-debug}

if [ $# -eq 1 ]; then
  num=$1
  resources_group=""
elif [ $# -eq 2 ]; then
  num=$1
  resources_group=$2
else
  echo "Usage: $0 <number> - Start number of databend-query with system-managed mode"
  exit 1
fi

if ! [[ "$num" =~ ^[0-9]*$ ]]; then
    echo "Error: Argument must be an integer."
    exit 1
fi

# Caveat: has to kill query first.
# `query` tries to remove its liveness record from meta before shutting down.
# If meta is stopped, `query` will receive an error that hangs graceful
# shutdown.
killall databend-query || true
sleep 3

killall databend-meta || true
sleep 3

for bin in databend-query databend-meta; do
	if test -n "$(pgrep $bin)"; then
		echo "The $bin is not killed. force killing."
		killall -9 $bin || true
	fi
done

# Wait for killed process to cleanup resources
sleep 1

echo 'Start Meta service HA cluster(3 nodes)...'

mkdir -p ./.databend/

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-1.toml >./.databend/meta-1.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9191

# wait for cluster formation to complete.
sleep 1

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-2.toml >./.databend/meta-2.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 28202

# wait for cluster formation to complete.
sleep 1

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-3.toml >./.databend/meta-3.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 28302

# wait for cluster formation to complete.
sleep 1

find_available_port() {
    local base_port=20000
    local max_port=65535
    local attempts=10

    for ((i=0; i<attempts; i++)); do
        port=$(( RANDOM % (max_port - base_port + 1) + base_port ))
        if ! lsof -i :$port >/dev/null 2>&1; then
            echo $port
            return
        fi
    done

    echo "Unable to find an available port after $attempts attempts" >&2
    exit 1
}


start_databend_query() {
    local http_port=$1
    local mysql_port=$2
    local log_dir=$3
    local resources_group=$4
    system_managed_config="./scripts/ci/deploy/config/databend-query-node-system-managed.toml"

    temp_file=$(mktemp)

    if [ -f "$system_managed_config" ]; then
        sed -e "s/flight_port/$(find_available_port)/g" \
            -e "s/admin_api_port/$(find_available_port)/g" \
            -e "s/metric_api_port/$(find_available_port)/g" \
            -e "s/mysql_port/${mysql_port}/g" \
            -e "s/clickhouse_port/$(find_available_port)/g" \
            -e "s/http_port/${http_port}/g" \
            -e "s/flight_sql_port/$(find_available_port)/g" \
            -e "s/query_logs/${log_dir}/g" \
            -e "s/resource_group/resource_group=\"${resources_group}\"/g" \
            "$system_managed_config" > "$temp_file"

        if [ $? -eq 0 ]; then
          echo "Start databend-query on port $http_port..."
          nohup target/${BUILD_PROFILE}/databend-query -c $temp_file --internal-enable-sandbox-tenant &

          echo "Waiting on databend-query 10 seconds..."
          python3 scripts/ci/wait_tcp.py --timeout 30 --port $http_port
        else
            echo "Error occurred during port replacement."
            rm -f "$temp_file"
            exit 1
        fi
    else
        echo "Error: system-managed config file is not exists."
        exit 1
    fi
}

if ! lsof -i :8000 >/dev/null 2>&1; then
  start_databend_query 8000 3307 "logs_1" $resources_group
  num=$(( num - 1 ))
fi

for (( i=0; i<$num; i++ ))
do
    http_port=$(find_available_port)
    mysql_port=$(find_available_port)
    start_databend_query $http_port $mysql_port "logs_$http_port" $resources_group
done
