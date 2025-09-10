#!/bin/bash

set -e

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "🔧 Installing databend_test_helper..."
pip install -e "$PROJECT_DIR/databend_test_helper"

echo "🚀 Running test cluster..."
python "$SCRIPT_DIR/test_cluster.py"
