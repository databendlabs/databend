#!/usr/bin/env python3
"""Merge external crate versions into min_compatible_versions.txt.

Resolves CalVer references so the output has only databend tag versions.
"""

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, "..")
SRC_DIR = os.path.join(BASE_DIR, "src")

INPUT = os.path.join(SRC_DIR, "min_compatible_versions.txt")
EXTERNAL = os.path.join(SRC_DIR, "external-meta-min-compatibles.txt")
OUTPUT = os.path.join(BASE_DIR, "generated", "resolved_min_compatibles.txt")


def is_calver(v: str) -> bool:
    """Check if a version string is a CalVer (e.g. "260205.0.0").

    Args:
        v: A version string like "1.2.770" or "260205.0.0".

    Returns:
        True if the first component is >= 200000 (indicating a CalVer).
    """
    try:
        return int(v.split(".")[0]) >= 200000
    except ValueError:
        return False


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


def load_external() -> dict[str, tuple[str, str]]:
    """Load external-meta-min-compatibles.txt.

    Returns:
        Mapping from external crate CalVer (e.g. "260205.0.0") to
        (min_server, min_client) where both values are version strings
        that may themselves be CalVer or SemVer.
    """
    return {r[0]: (r[1], r[2]) for r in parse_tsv(EXTERNAL, 3)}


def build_calver_to_tag() -> dict[str, str]:
    """Scan INPUT to map each CalVer to the first repo tag that uses it.

    Returns:
        Mapping from CalVer (e.g. "260205.0.0") to the first databend
        repo tag (e.g. "1.2.880") whose 4th column matches that CalVer.
    """
    mapping: dict[str, str] = {}
    for r in parse_tsv(INPUT, 4):
        if r[3] != "-":
            mapping.setdefault(r[3], r[0])
    return mapping


def resolve_calver(v: str, calver_to_tag: dict[str, str]) -> str:
    """Translate a CalVer to a repo tag version if possible.

    Args:
        v: A version string, either SemVer ("1.2.770") or CalVer ("260205.0.0").
        calver_to_tag: Mapping from CalVer to repo tag.

    Returns:
        The corresponding repo tag if v is a CalVer found in the mapping,
        otherwise v unchanged.
    """
    if is_calver(v):
        return calver_to_tag.get(v, v)
    return v


def resolve_row(
    parts: list[str],
    ext: dict[str, tuple[str, str]],
    calver_to_tag: dict[str, str],
) -> tuple[str, str, str, str] | None:
    """Resolve a single row from the input file.

    For rows where min_server/min_client are "-", looks up the 4th column
    (CalVer) in `ext` to get the external crate's min versions, then
    translates any CalVer values to repo tags.

    Args:
        parts: A row from INPUT, e.g. ["1.2.880", "-", "-", "260205.0.0"].
        ext: Output of load_external().
        calver_to_tag: Output of build_calver_to_tag().

    Returns:
        (tag, min_server, min_client, calver) with all versions resolved
        to repo tags, or None if the row cannot be resolved.
    """
    tag = parts[0]
    server, client = parts[1], parts[2]
    calver = parts[3] if len(parts) >= 4 else "-"

    if server == "-" or client == "-":
        if calver == "-" or calver not in ext:
            return None
        server, client = ext[calver]
        server = resolve_calver(server, calver_to_tag)
        client = resolve_calver(client, calver_to_tag)

    return (tag, server, client, calver)


def write_output(rows: list[tuple[str, str, str, str]]) -> None:
    """Write resolved rows to OUTPUT file.

    Args:
        rows: List of (tag, min_server, min_client, calver) tuples,
              all versions already resolved to repo tags.
    """
    with open(OUTPUT, "w") as out:
        out.write(f"{'tag':<16}{'min_meta_server':<20}{'min_meta_client':<20}{'external_meta_crate':<20}\n")
        out.write("-" * 76 + "\n")
        for tag, server, client, calver in rows:
            out.write(f"{tag:<16}{server:<20}{client:<20}{calver:<20}\n")


def main():
    ext = load_external()
    calver_to_tag = build_calver_to_tag()

    resolved = []
    for parts in parse_tsv(INPUT, 3):
        row = resolve_row(parts, ext, calver_to_tag)
        if row:
            resolved.append(row)

    write_output(resolved)
    print(f"Written to {OUTPUT}", file=sys.stderr)


if __name__ == "__main__":
    main()
