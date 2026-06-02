#!/usr/bin/env python3
"""Re-key the databend-meta `raft-compat` snapshot by databend version.

Raft inter-node compatibility is computed by the `RaftSpec` logic in the
databend-meta crate (not this repo's git-tag pipeline), so the authoritative
source is that crate's `raft-compat` binary, snapshotted in
`src/raft-peer-compatibles.txt`:

    1.2.547      [0.0.120, ∞)
    260512.0.0   [1.2.547, ∞)

That snapshot is keyed by databend-meta version. databend-meta shared semver
with databend up to 1.2.879 (monorepo era), then split into its own repo and
switched to CalVer (`YYMMDD.x.x`). This script re-keys the table by databend
version, matching `meta_to_query.txt`:

  - semver databend-meta versions (<= 1.2.879) are databend versions 1:1.
  - CalVer databend-meta versions map to the databend versions that bundle
    them, via the `MetaService` column of `min_compatible_versions.txt`. CalVer
    minors carry no compatibility change, so only the major is matched.

Output (stdout):

    meta: 1.2.547          peer: [0.0.120, ~)

`make meta_to_meta.txt` runs this against the committed snapshot. Refresh the
snapshot from a databend-meta checkout when raft features change:

    cargo run -p databend-meta-version --bin raft-compat > src/raft-peer-compatibles.txt
"""

import os
import re

from packaging.version import Version

from tsv import parse_tsv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(SCRIPT_DIR, "..", "src")

RAFT_COMPAT = os.path.join(SRC_DIR, "raft-peer-compatibles.txt")
MIN_COMPAT = os.path.join(SRC_DIR, "min_compatible_versions.txt")

LINE = re.compile(r"^(\S+)\s+\[([^,]+),\s*([^)]+)\)\s*$")

# databend-meta releases at or above this major use CalVer (YYMMDD), not semver.
CALVER_MIN_MAJOR = 100000
EXT_PREFIX = "ext:"


def fmt_range(since: str, until: str) -> str:
    """Render a half-open range, matching `find_compat.py`'s `~` for unbounded."""
    until = "~" if until == "∞" else until
    return f"[{since}, {until})"


def major_of(version: str) -> int:
    """The integer major component (`260512` from `260512.0.0`, `1` from `1.2.3`).

    Strips an `ext:` prefix; for CalVer this is the whole `YYMMDD` date.
    """
    if version.startswith(EXT_PREFIX):
        version = version[len(EXT_PREFIX):]
    return int(version.split(".")[0])


def load_calver_to_databend() -> dict[int, list[str]]:
    """Map each databend-meta CalVer major to the databend versions bundling it.

    The `MetaService` column of `min_compatible_versions.txt` is the
    databend-meta version a databend release runs; split-era rows carry an
    `ext:<CalVer>` value there.
    """
    mapping: dict[int, list[str]] = {}
    for tag, _min_client, min_server in parse_tsv(MIN_COMPAT, 3):
        if min_server.startswith(EXT_PREFIX):
            mapping.setdefault(major_of(min_server), []).append(tag)
    return mapping


def parse_raft_compat() -> list[tuple[str, str, str]]:
    """Parse the snapshot into `(meta_version, since, until)` rows."""
    rows = []
    with open(RAFT_COMPAT) as f:
        for line in f:
            if not line.strip():
                continue
            m = LINE.match(line.rstrip("\n"))
            if not m:
                raise ValueError(f"unrecognized raft-compat line: {line!r}")
            rows.append(m.groups())
    return rows


def main() -> None:
    calver_to_databend = load_calver_to_databend()

    # (databend_version, since, until). CalVer rows expand to every databend
    # version bundling that major; dedup on major so minors don't re-emit.
    out: list[tuple[str, str, str]] = []
    seen_majors: set[int] = set()
    for meta_version, since, until in parse_raft_compat():
        major = major_of(meta_version)
        if major < CALVER_MIN_MAJOR:
            out.append((meta_version, since, until))
            continue
        if major in seen_majors:
            continue
        seen_majors.add(major)
        for databend_version in calver_to_databend.get(major, []):
            out.append((databend_version, since, until))

    out.sort(key=lambda r: Version(r[0]))
    for version, since, until in out:
        print(f"meta: {version:<16} peer: {fmt_range(since, until)}")


if __name__ == "__main__":
    main()
