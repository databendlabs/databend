#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "=== positional ? params ==="
curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "SELECT ?, ?, ?", "params": [42, "hello", true], "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.data, .error'

echo "=== named :name params ==="
curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "SELECT :age, :name", "params": {"age": 30, "name": "Alice"}, "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.data, .error'

echo "=== null and bool params ==="
curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "SELECT ?, ?", "params": [null, false], "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.data, .error'

echo "=== insert with params ==="
curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "CREATE TABLE IF NOT EXISTS t_params_test(a INT, b VARCHAR, c BOOLEAN) ENGINE=Memory"}' > /dev/null

curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "INSERT INTO t_params_test VALUES (?, ?, ?)", "params": [1, "row1", true], "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.error'

curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "INSERT INTO t_params_test VALUES (?, ?, ?)", "params": [2, "row2", false], "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.error'

curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM t_params_test ORDER BY a", "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.data, .error'

echo "=== where clause with params ==="
curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM t_params_test WHERE a = ? AND b = ?", "params": [1, "row1"], "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.data, .error'

echo "=== sql injection prevention ==="
curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "SELECT ?", "params": ["Robert'"'"'); DROP TABLE t_params_test;--"], "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.data, .error'

curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "SELECT COUNT(*) FROM t_params_test", "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.data, .error'

echo "=== backward compatibility (no params) ==="
curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "SELECT 1", "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.data, .error'

curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "DROP TABLE IF EXISTS t_params_test"}' > /dev/null

echo "=== error: not enough params ==="
curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "SELECT ?, ?", "params": [42], "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.error.code'

echo "=== error: missing named param ==="
curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  --header 'Content-Type: application/json' \
  -d '{"sql": "SELECT :name", "params": {"age": 30}, "pagination": {"wait_time_secs": 6}}' \
  | jq -r '.error.code'
