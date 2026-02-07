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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, "..")
VERSIONS_FILE = os.path.join(BASE_DIR, "generated", "resolved_min_compatibles.txt")


def parse_tsv(path: str, min_columns: int) -> list[list[str]]:
    """Parse a whitespace-separated file, skipping headers and separators.

    Args:
        path: Path to the file.
        min_columns: Minimum number of columns a row must have to be included.

    Returns:
        List of rows, each row is a list of string fields.
        Header lines (starting with a letter) and separator lines (starting
        with "-") are excluded.
    """
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line[0].isalpha() or line.startswith("-"):
                continue
            parts = line.split()
            if len(parts) >= min_columns:
                rows.append(parts)
    return rows


def load_versions() -> list[tuple[Version, Version, Version]]:
    """Load the resolved min_compatible_versions.txt.

    Returns:
        List of (tag, min_server, min_client) tuples parsed as Version
        objects. Rows with invalid version strings are skipped.
    """
    rows = []
    for parts in parse_tsv(VERSIONS_FILE, 3):
        try:
            rows.append((Version(parts[0]), Version(parts[1]), Version(parts[2])))
        except InvalidVersion:
            continue
    return rows


def find_row(
    rows: list[tuple[Version, Version, Version]], ver: Version
) -> tuple[Version, Version, Version] | None:
    """Find the row whose tag (column 0) matches ver.

    Args:
        rows: Output of load_versions().
        ver: The version to search for.

    Returns:
        The matching (tag, min_server, min_client) tuple, or None if not found.
    """
    for r in rows:
        if r[0] == ver:
            return r
    return None


def find_first_exceeding(
    rows: list[tuple[Version, Version, Version]], col: int, threshold: Version
) -> Version | None:
    """Find the tag of the first row where the given column exceeds threshold.

    Args:
        rows: Output of load_versions().
        col: Column index to compare (1 = min_server, 2 = min_client).
        threshold: The version to compare against.

    Returns:
        The tag (column 0) of the first row where rows[col] > threshold,
        or None if no row exceeds it.
    """
    for r in rows:
        if r[col] > threshold:
            return r[0]
    return None


def compat_range(
    rows: list[tuple[Version, Version, Version]],
    ver: Version,
    since_col: int,
    until_col: int,
) -> tuple[Version, Version | None]:
    """Compute the [since, until) compatible range for a given version.

    Args:
        rows: Output of load_versions().
        ver: The version to look up.
        since_col: Column index for the "since" bound (1 or 2).
        until_col: Column index to scan for the "until" bound (1 or 2).

    Returns:
        (since, until) where since is the minimum compatible version and
        until is the first incompatible version (or None for open-ended).
    """
    row = find_row(rows, ver)
    if row is None:
        return None, None

    since = row[since_col]
    until = find_first_exceeding(rows, until_col, ver)
    return since, until


def fmt_range(since: Version, until: Version | None) -> str:
    """Format a [since, until) range as a string.

    Args:
        since: The inclusive lower bound.
        until: The exclusive upper bound, or None for open-ended.

    Returns:
        A string like "[1.2.259, 1.2.880)" or "[1.2.770, ~)".
    """
    return f"[{since}, {until or '~'})"


def query_compat(rows: list[tuple[Version, Version, Version]], query_ver: str) -> None:
    """Print the [since, until) range of compatible metasrv for a query version.

    Args:
        rows: Output of load_versions().
        query_ver: A databend-query version string, e.g. "1.2.600".
    """
    ver = Version(query_ver)
    since, until = compat_range(rows, ver, since_col=1, until_col=2)

    if since is None:
        print(f"query version {query_ver} not found")
        sys.exit(1)

    print(f"query(client) version: {query_ver}")
    print(f"compatible metasrv:    {fmt_range(since, until)}")


def metasrv_compat(rows: list[tuple[Version, Version, Version]], metasrv_ver: str) -> None:
    """Print the [since, until) range of compatible query for a metasrv version.

    Args:
        rows: Output of load_versions().
        metasrv_ver: A meta-service version string, e.g. "1.2.400".
    """
    ver = Version(metasrv_ver)
    since, until = compat_range(rows, ver, since_col=2, until_col=1)

    if since is None:
        print(f"metasrv version {metasrv_ver} not found")
        sys.exit(1)

    print(f"metasrv version:       {metasrv_ver}")
    print(f"compatible query:      {fmt_range(since, until)}")


def all_query_compat(rows: list[tuple[Version, Version, Version]]) -> None:
    """Print the compatible metasrv range for every query version.

    Args:
        rows: Output of load_versions().
    """
    for tag, _, _ in rows:
        since, until = compat_range(rows, tag, since_col=1, until_col=2)
        print(f"query: {str(tag):<16} meta: {fmt_range(since, until)}")


def all_meta_compat(rows: list[tuple[Version, Version, Version]]) -> None:
    """Print the compatible query range for every metasrv version.

    Args:
        rows: Output of load_versions().
    """
    for tag, _, _ in rows:
        since, until = compat_range(rows, tag, since_col=2, until_col=1)
        print(f"meta: {str(tag):<16} query: {fmt_range(since, until)}")


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

    rows = load_versions()

    if args.query:
        query_compat(rows, args.query)
    if args.meta:
        metasrv_compat(rows, args.meta)
    if args.all_query:
        all_query_compat(rows)
    if args.all_meta:
        all_meta_compat(rows)


if __name__ == "__main__":
    main()
