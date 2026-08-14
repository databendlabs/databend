#!/bin/bash

set -e

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR"

echo "Running Databend logging tests..."

echo "1. Setting up test data..."
source setup_test_data.sh

echo "2. Testing external catalog fields API..."
bash ./test_external_catalog_fields.sh

echo "3. Running log table checks..."
./check_logs_table.sh

echo "4. Running permissions tests..."
./test_permissions.sh

echo "All tests completed successfully!"
