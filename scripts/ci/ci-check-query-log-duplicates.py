#!/usr/bin/env python3
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.
#
# Validates that no query_id appears with duplicate Start or duplicate
# Finish/Error/Aborted entries in the query-details log files.
# Exit 0 if clean, exit 1 if duplicates found.

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

LOG_TYPE_START = 1
LOG_TYPE_FINISH = 2
LOG_TYPE_ERROR = 3
LOG_TYPE_ABORTED = 4

DEFAULT_LOG_DIR = ".databend/logs_1/query-details"


def scan_log_files(log_dir):
    counts = defaultdict(lambda: {"start": 0, "terminal": 0})
    total_lines = 0
    parse_errors = 0

    log_path = Path(log_dir)
    if not log_path.exists():
        print(f"Log directory not found: {log_dir}, skipping check.")
        return 0

    for f in sorted(log_path.iterdir()):
        if not f.is_file():
            continue
        with open(f, "r") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                total_lines += 1
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    parse_errors += 1
                    continue

                query_id = entry.get("query_id", "")
                log_type = entry.get("log_type")
                if not query_id or log_type is None:
                    continue

                if log_type == LOG_TYPE_START:
                    counts[query_id]["start"] += 1
                elif log_type in (LOG_TYPE_FINISH, LOG_TYPE_ERROR, LOG_TYPE_ABORTED):
                    counts[query_id]["terminal"] += 1

    print(f"Scanned {total_lines} log entries ({parse_errors} parse errors)")

    violations = []
    for qid, c in counts.items():
        if c["start"] > 1:
            violations.append(f"  query_id={qid}: {c['start']} Start entries")
        if c["terminal"] > 1:
            violations.append(f"  query_id={qid}: {c['terminal']} Finish/Error/Aborted entries")

    if violations:
        print(f"FAILED: found {len(violations)} duplicate query log violations:")
        for v in violations:
            print(v)
        return 1

    print(f"OK: {len(counts)} unique query_ids, no duplicates.")
    return 0


if __name__ == "__main__":
    log_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_LOG_DIR
    sys.exit(scan_log_files(log_dir))
