#!/usr/bin/env python3
"""Find compatible version ranges between databend-query and meta-service.

Usage:
    python3 find_compat.py -q 1.2.600
    python3 find_compat.py -m 1.2.400
    python3 find_compat.py --all-query
    python3 find_compat.py --all-meta
"""

import argparse
import os
import sys
from packaging.version import InvalidVersion
from packaging.version import Version

from tsv import parse_tsv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, "..")
VERSIONS_FILE = os.path.join(BASE_DIR, "generated", "resolved_min_compatibles.txt")

# Column indices in resolved output: tag(0), min_client(1), min_server(2)
COL_CLIENT = 1
COL_SERVER = 2


def load_resolved_versions() -> list[tuple[Version, Version, Version]]:
    """Load resolved_min_compatibles.txt as (tag, min_client, min_server) tuples."""
    rows = []
    for parts in parse_tsv(VERSIONS_FILE, 3):
        try:
            rows.append((Version(parts[0]), Version(parts[1]), Version(parts[2])))
        except InvalidVersion:
            continue
    return rows


def find_compat_range(
    rows: list[tuple[Version, Version, Version]],
    ver: Version,
    since_col: int,
    until_col: int,
) -> tuple[Version | None, Version | None]:
    """Find the [since, until) compatible range for a given version.

    since_col: column whose value gives the lower bound.
    until_col: column scanned to find the first row exceeding ver.
    """
    row = next((r for r in rows if r[0] == ver), None)
    if row is None:
        return None, None

    since = row[since_col]
    until = next((r[0] for r in rows if r[until_col] > ver), None)
    return since, until


def fmt_range(since: Version, until: Version | None) -> str:
    return f"[{since}, {until or '~'})"


def print_version_compat(
    rows: list[tuple[Version, Version, Version]],
    ver_str: str,
    since_col: int,
    until_col: int,
    label: str,
    peer_label: str,
) -> None:
    """Print the compatible range for a single version."""
    ver = Version(ver_str)
    since, until = find_compat_range(rows, ver, since_col, until_col)
    if since is None:
        print(f"{label} version {ver_str} not found")
        sys.exit(1)
    print(f"{label + ' version:':<23}{ver_str}")
    print(f"{'compatible ' + peer_label + ':':<23}{fmt_range(since, until)}")


def print_all_version_compat(
    rows: list[tuple[Version, Version, Version]],
    since_col: int,
    until_col: int,
    label: str,
    peer_label: str,
) -> None:
    """Print the compatible range for every version."""
    for tag, _, _ in rows:
        since, until = find_compat_range(rows, tag, since_col, until_col)
        print(f"{label}: {str(tag):<16} {peer_label}: {fmt_range(since, until)}")


def main():
    p = argparse.ArgumentParser(description="Find compatible version ranges")
    p.add_argument("-q", "--query", help="databend-query version to check")
    p.add_argument("-m", "--meta", help="meta-service version to check")
    p.add_argument("--all-query", action="store_true",
                   help="print compatible metasrv for all query versions")
    p.add_argument("--all-meta", action="store_true",
                   help="print compatible query for all metasrv versions")
    args = p.parse_args()

    if not args.query and not args.meta and not args.all_query and not args.all_meta:
        p.error("specify --query, --meta, --all-query, or --all-meta")

    rows = load_resolved_versions()

    if args.query:
        print_version_compat(rows, args.query, COL_SERVER, COL_CLIENT, "query", "metasrv")
    if args.meta:
        print_version_compat(rows, args.meta, COL_CLIENT, COL_SERVER, "metasrv", "query")
    if args.all_query:
        print_all_version_compat(rows, COL_SERVER, COL_CLIENT, "query", "meta")
    if args.all_meta:
        print_all_version_compat(rows, COL_CLIENT, COL_SERVER, "meta", "query")


if __name__ == "__main__":
    main()
