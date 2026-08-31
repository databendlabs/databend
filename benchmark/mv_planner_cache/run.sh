#!/usr/bin/env bash
# Copyright 2021 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
ITERATIONS="${ITERATIONS:-20}"
START_SERVER="${START_SERVER:-1}"
DB="mv_planner_cache"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/.databend/logs_1}"
TMP_DIR="$(mktemp -d)"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if ! command -v bendsql >/dev/null 2>&1; then
    echo "bendsql is required" >&2
    exit 1
fi

if [[ "$START_SERVER" == "1" ]]; then
    echo "Starting a fresh ${BUILD_PROFILE} standalone deployment (existing local services may be stopped)."
    rm -rf .databend
    BUILD_PROFILE="$BUILD_PROFILE" bash ./scripts/ci/deploy/databend-query-standalone.sh
fi

export BENDSQL_DSN="databend://root:@127.0.0.1:8000/${DB}?sslmode=disable"
bendsql < benchmark/mv_planner_cache/create.sql >/dev/null

collect_planner_logs() {
    if [[ ! -d "$LOG_DIR" ]]; then
        return 0
    fi
    find "$LOG_DIR" -type f -name 'databend-query-*' -print0 \
        | sort -z \
        | xargs -0r cat
}

run_phase() {
    local name="$1"
    local planner_cache="$2"
    local iterations="$3"
    local before after before_lines

    collect_planner_logs >"$TMP_DIR/${name}.before"
    before_lines=$(wc -l <"$TMP_DIR/${name}.before")
    {
        printf 'SET enable_planner_cache = %s;\nSET enable_materialized_view_rewrite = 1;\n' "$planner_cache"
        for _ in $(seq 1 "$iterations"); do
            cat benchmark/mv_planner_cache/query.sql
        done
    } | bendsql >/dev/null
    sleep 1
    collect_planner_logs >"$TMP_DIR/${name}.after"
    after=$(wc -l <"$TMP_DIR/${name}.after")
    if (( after > before_lines )); then
        tail -n +$((before_lines + 1)) "$TMP_DIR/${name}.after" >"$TMP_DIR/${name}.logs"
    else
        : >"$TMP_DIR/${name}.logs"
    fi
}

echo "Running ${ITERATIONS} cache-disabled planning iterations..."
run_phase cache_disabled 0 "$ITERATIONS"

echo "Running one cache-miss planning iteration..."
run_phase cache_miss 1 1

echo "Running ${ITERATIONS} cache-hit planning iterations..."
run_phase cache_hit 1 "$ITERATIONS"

printf '\nPlanner timing records:\n'
for phase in cache_disabled cache_miss cache_hit; do
    echo "--- ${phase} ---"
    grep -E 'Logical plan (construction completed|retrieved from cache)' \
        "$TMP_DIR/${phase}.logs" || true
done

python3 - "$TMP_DIR" <<'PY'
import re
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
patterns = {
    "cache_disabled": re.compile(
        r"Logical plan construction completed.*?cache_context_us=(\d+).*?bind_us=(\d+).*?optimize_us=(\d+).*?total_us=(\d+)"
    ),
    "cache_miss": re.compile(
        r"Logical plan construction completed.*?cache_context_us=(\d+).*?bind_us=(\d+).*?optimize_us=(\d+).*?total_us=(\d+)"
    ),
    "cache_hit": re.compile(
        r"Logical plan retrieved from cache.*?cache_context_us=(\d+).*?cache_lookup_us=(\d+).*?total_us=(\d+)"
    ),
}

for phase, pattern in patterns.items():
    rows = []
    log_file = root / (phase + ".logs")
    for line in log_file.read_text(errors="replace").splitlines():
        match = pattern.search(line)
        if match:
            rows.append(tuple(map(int, match.groups())))

    if not rows:
        print(phase + ": no planner timing records found")
        continue

    totals = [row[-1] for row in rows]
    p95_index = max(0, min(len(totals) - 1, int(len(totals) * 0.95) - 1))
    print(
        phase
        + ": count="
        + str(len(rows))
        + " avg_us="
        + format(statistics.mean(totals), ".1f")
        + " p50_us="
        + format(statistics.median(totals), ".1f")
        + " p95_us="
        + str(sorted(totals)[p95_index])
    )
    if phase == "cache_hit":
        print(
            phase
            + ": avg_cache_context_us="
            + format(statistics.mean(row[0] for row in rows), ".1f")
            + " avg_cache_lookup_us="
            + format(statistics.mean(row[1] for row in rows), ".1f")
        )
    else:
        print(
            phase
            + ": avg_cache_context_us="
            + format(statistics.mean(row[0] for row in rows), ".1f")
            + " avg_bind_us="
            + format(statistics.mean(row[1] for row in rows), ".1f")
            + " avg_optimize_us="
            + format(statistics.mean(row[2] for row in rows), ".1f")
        )
PY
