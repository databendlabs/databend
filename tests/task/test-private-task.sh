#!/bin/bash

set -e

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit

echo "Cleaning up previous runs"

killall -9 databend-query || true
killall -9 databend-meta || true
rm -rf .databend

echo "Starting Databend Query cluster with 2 nodes enable private task"

for node in 1 2; do
    CONFIG_FILE="./scripts/ci/deploy/config/databend-query-node-${node}.toml"

    echo "Appending history table config to node-${node}"
    cat ./tests/task/private_task.toml >> "$CONFIG_FILE"
done

# Start meta cluster (3 nodes - needed for HA)
echo 'Start Meta service HA cluster(3 nodes)...'

mkdir -p ./.databend/

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-1.toml >./.databend/meta-1.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9191

sleep 1

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-2.toml >./.databend/meta-2.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 28202

sleep 1

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-3.toml >./.databend/meta-3.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 28302

sleep 1

# Start only 2 query nodes
echo 'Start databend-query node-1'
nohup env RUST_BACKTRACE=1 target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml --internal-enable-sandbox-tenant >./.databend/query-1.out 2>&1 &

echo "Waiting on node-1..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9091

echo 'Start databend-query node-2'
env "RUST_BACKTRACE=1" nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-2.toml --internal-enable-sandbox-tenant >./.databend/query-2.out 2>&1 &

echo "Waiting on node-2..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9092

echo "Started 2-node cluster with private task enabled..."

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TABLE t1 (c1 int)\"}")
create_table_query_id=$(echo $response | jq -r '.id')
echo "Create Table Query ID: $create_table_query_id"

response1=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_1 WAREHOUSE = 'mywh' SCHEDULE = 1 SECOND AS insert into t1 values(0)\"}")
create_task_1_query_id=$(echo $response1 | jq -r '.id')
echo "Create Task 1 Query ID: $create_task_1_query_id"

response2=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_2 WAREHOUSE = 'mywh' SCHEDULE = 5 SECOND AS insert into t1 values(1)\"}")
create_task_2_query_id=$(echo $response2 | jq -r '.id')
echo "Create Task 2 ID: $create_task_2_query_id"

response3=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_3 WAREHOUSE = 'mywh' AFTER 'my_task_1', 'my_task_2' AS insert into t1 values(2)\"}")
create_task_3_query_id=$(echo $response3 | jq -r '.id')
echo "Create Task 3 ID: $create_task_3_query_id"

sleep 1

response4=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_3 RESUME\"}")
alter_task_3_query_id=$(echo $response4 | jq -r '.id')
echo "Resume Task 3 ID: $alter_task_3_query_id"

sleep 1

# Check Task Afters 1

response5=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK my_task_1\"}")
execute_task_1_query_id=$(echo $response5 | jq -r '.id')
echo "Execute Task 1 ID: $execute_task_1_query_id"

response6=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK my_task_2\"}")
execute_task_2_query_id=$(echo $response6 | jq -r '.id')
echo "Execute Task 2 ID: $execute_task_2_query_id"

sleep 5

response7=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")

actual=$(echo "$response7" | jq -c '.data')
expected='[["0"],["1"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

# Check Task Afters With Schedule Root

response5=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK my_task_1\"}")
execute_task_1_query_id=$(echo $response5 | jq -r '.id')
echo "Execute Task 1 ID: $execute_task_1_query_id"

sleep 5

response7=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")

actual=$(echo "$response7" | jq -c '.data')
expected='[["0"],["0"],["1"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response8=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_2 RESUME\"}")
alter_task_2_query_id=$(echo $response8 | jq -r '.id')
echo "Resume Task 2 ID: $alter_task_2_query_id"

sleep 10

response9=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")

actual=$(echo "$response9" | jq -c '.data')
expected='[["0"],["0"],["1"],["1"],["2"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

# Test whether the schedule can be restored after restart

killall -9 databend-query || true

echo 'Start databend-query node-1'
nohup env RUST_BACKTRACE=1 target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml --internal-enable-sandbox-tenant >./.databend/query-1.out 2>&1 &

echo "Waiting on node-1..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9091

echo 'Start databend-query node-2'
env "RUST_BACKTRACE=1" nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-2.toml --internal-enable-sandbox-tenant >./.databend/query-2.out 2>&1 &

echo "Waiting on node-2..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9092

echo "Started 2-node cluster with private task enabled..."

sleep 7

response9=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")

actual=$(echo "$response9" | jq -c '.data')
expected='[["0"],["0"],["1"],["1"],["1"],["2"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

# Show Task
response10=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"Describe Task my_task_1\"}")
state10=$(echo "$response10" | jq -r '.state')
if [ "$state10" != "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
fi
actual=$(echo "$response10" | jq -c '.data')
echo "\n\nDescribe Task my_task_1: $actual"

response11=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SHOW TASKS\"}")
state11=$(echo "$response11" | jq -r '.state')
if [ "$state10" != "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
fi
actual=$(echo "$response11" | jq -c '.data')
echo "\n\nSHOW TASKS: $actual"

response12=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT * FROM system.task_history\"}")
state12=$(echo "$response12" | jq -r '.state')
if [ "$state10" != "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
fi
actual=$(echo "$response12" | jq -c '.data')
echo "\n\nSELECT * FROM system.task_history: $actual"

# Drop Task
response13=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"DROP TASK my_task_1\"}")
state13=$(echo "$response13" | jq -r '.state')
if [ "$state13" != "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
else
  echo "✅ Passed"
fi

response14=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK my_task_1\"}")
state14=$(echo "$response14" | jq -r '.state')
if [ "$state14" = "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
else
  echo "✅ Passed"
fi

response15=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TABLE t2 (c1 int)\"}")
create_table_query_id_1=$(echo $response15 | jq -r '.id')
echo "Create Table Query ID: $create_table_query_id_1"

response16=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_4 WAREHOUSE = 'mywh' SCHEDULE = USING CRON '*/5 * * * * ?' AS insert into t2 values(0)\"}")
create_task_4_query_id=$(echo $response16 | jq -r '.id')
echo "Create Task 4 Query ID: $create_task_4_query_id"

sleep 1

response17=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_4 RESUME\"}")
alter_task_4_query_id=$(echo $response17 | jq -r '.id')
echo "Resume Task 4 ID: $alter_task_4_query_id"

sleep 11

response18=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t2 ORDER BY c1\"}")

actual=$(echo "$response18" | jq -c '.data')
expected='[["0"],["0"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi
