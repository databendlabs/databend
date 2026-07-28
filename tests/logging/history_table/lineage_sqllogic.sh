#!/bin/bash

# Shared sqllogictest runner for lineage history setup and assertions.
LINEAGE_HISTORY_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LINEAGE_HISTORY_REPO_ROOT="$(cd "$LINEAGE_HISTORY_SCRIPT_DIR/../../.." && pwd)"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"

run_lineage_suite() {
  local suite_dir="$1"
  (
    cd "$LINEAGE_HISTORY_REPO_ROOT"
    "target/${BUILD_PROFILE}/databend-sqllogictests" \
      --suites tests/logging/history_table/sqllogic \
      --run_dir "$suite_dir" \
      --handlers http \
      --parallel 1
  )
}
