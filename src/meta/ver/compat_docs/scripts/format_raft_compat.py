#!/usr/bin/env python3
"""Format the databend-meta `raft-compat` binary output as a compat table.

Raft inter-node compatibility is computed by the `RaftSpec` logic in the
databend-meta crate, not by this repo's git-tag pipeline, so the authoritative
source is that crate's `raft-compat` binary. This script only reformats its
output to match the visual style of `meta_to_query.txt` / `query_to_meta.txt`.

Reads `raft-compat` output on stdin:

    1.2.547      [0.0.120, ∞)

and writes the peer-compatibility table to stdout:

    meta: 1.2.547          peer: [0.0.120, ~)

`make meta_to_meta.txt` runs this against the committed snapshot
`src/raft-peer-compatibles.txt`. Refresh that snapshot from a databend-meta
checkout when raft features change:

    cargo run -p databend-meta-version --bin raft-compat > src/raft-peer-compatibles.txt
"""

import re
import sys

LINE = re.compile(r"^(\S+)\s+\[([^,]+),\s*([^)]+)\)\s*$")


def fmt_range(since: str, until: str) -> str:
    """Render a half-open range, matching `find_compat.py`'s `~` for unbounded."""
    until = "~" if until == "∞" else until
    return f"[{since}, {until})"


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        m = LINE.match(line.rstrip("\n"))
        if not m:
            raise ValueError(f"unrecognized raft-compat line: {line!r}")
        node, since, until = m.groups()
        print(f"meta: {node:<16} peer: {fmt_range(since, until)}")


if __name__ == "__main__":
    main()
