#!/bin/bash

set -e

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR"

echo "Running Databend logging tests..."

echo "1. Setting up test data..."
source setup_test_data.sh

echo "2. Running log table checks..."
./check_logs_table.sh

echo "3. Running ETL failure and recovery tests..."
python3 ./check_history_etl.py

echo "4. Running permissions tests..."
./test_permissions.sh

echo "All tests completed successfully!"
