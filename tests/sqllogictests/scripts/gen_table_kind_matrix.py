#!/usr/bin/env python3
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

"""Generate the table-kind x write-path matrix sqllogictest.

Fuse tables come in several kinds (regular, TRANSIENT, TEMP) that share the
same write paths (insert, update, delete, replace, merge, overwrite, schema
evolution, compaction, transactions). Each kind flips a few switches deep in
the commit pipeline (retention, snapshot purge, meta storage), and bugs have
repeatedly appeared where one write path did not account for one kind:
transient tables inside explicit transactions, transient auto-compaction
failing silently, CREATE TEMP TABLE AS SELECT, ... Historically each kind was
tested by whoever added it, on the paths they thought of.

This script runs the *same* sequence of write operations against every kind
and asserts the same observable results, so a kind-specific divergence shows
up as a diff. Expected values follow from the operations themselves.

Regenerate with:

    python3 tests/sqllogictests/scripts/gen_table_kind_matrix.py

and commit the resulting `.test` file together with this script.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

OUTPUT = (
    Path(__file__).resolve().parents[1]
    / "suites"
    / "base"
    / "09_fuse_engine"
    / "09_0060_table_kind_matrix.test"
)


@dataclass(frozen=True)
class Kind:
    name: str
    create: str  # keyword(s) between CREATE OR REPLACE and TABLE
    # Whether snapshot history is kept. A TRANSIENT table purges everything but the
    # current snapshot on every commit, regardless of the session retention settings.
    keeps_history: bool


KINDS = [
    Kind("regular", "", keeps_history=True),
    Kind("transient", "TRANSIENT", keeps_history=False),
    Kind("temp", "TEMP", keeps_history=True),
]


def gen_kind(out: list[str], kind: Kind):
    t = f"tkm_{kind.name}"
    src = f"{t}_src"
    kw = f"{kind.create} " if kind.create else ""

    def ok(sql: str):
        out.append("statement ok")
        out.append(sql)
        out.append("")

    def query(columns: str, sql: str, *rows: str):
        out.append(f"query {columns}")
        out.append(sql)
        out.append("----")
        out.extend(rows)
        out.append("")

    def error(code: str, sql: str):
        out.append(f"statement error {code}")
        out.append(sql)
        out.append("")

    out.append(f"## {kind.name} table")
    ok(f"CREATE OR REPLACE {kw}TABLE {t}(a INT, b STRING)")

    # Two inserts, two blocks.
    ok(f"INSERT INTO {t} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    ok(f"INSERT INTO {t} VALUES (4, 'd'), (5, 'e'), (6, 'f')")
    query("II", f"SELECT count(*), sum(a) FROM {t}", "6 21")
    query("I", f"SELECT count(*) FROM fuse_block('default', '{t}')", "2")

    # Row-level mutations.
    ok(f"UPDATE {t} SET b = 'x' WHERE a = 2")
    query("I", f"SELECT count(*) FROM {t} WHERE b = 'x'", "1")
    ok(f"DELETE FROM {t} WHERE a = 6")
    query("II", f"SELECT count(*), sum(a) FROM {t}", "5 15")

    # REPLACE INTO: one update (a = 1), one insert (a = 7).
    ok(f"REPLACE INTO {t} ON (a) VALUES (1, 'r'), (7, 'g')")
    query("II", f"SELECT count(*), sum(a) FROM {t}", "6 22")
    query("T", f"SELECT b FROM {t} WHERE a = 1", "r")

    # MERGE INTO from a regular source: one update (a = 2), one insert (a = 8).
    ok(f"CREATE OR REPLACE TABLE {src}(a INT, b STRING)")
    ok(f"INSERT INTO {src} VALUES (2, 'm'), (8, 'h')")
    ok(
        f"MERGE INTO {t} USING {src} AS s ON {t}.a = s.a "
        "WHEN MATCHED THEN UPDATE SET b = s.b "
        "WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b)"
    )
    query("II", f"SELECT count(*), sum(a) FROM {t}", "7 30")
    query("T", f"SELECT b FROM {t} WHERE a = 2", "m")

    # Schema evolution: add with default, rename, drop.
    ok(f"ALTER TABLE {t} ADD COLUMN c INT DEFAULT 9")
    query("I", f"SELECT sum(c) FROM {t}", "63")
    ok(f"ALTER TABLE {t} RENAME COLUMN c TO d")
    query("I", f"SELECT sum(d) FROM {t}", "63")
    ok(f"ALTER TABLE {t} DROP COLUMN d")
    error("1065", f"SELECT d FROM {t}")
    query("II", f"SELECT count(*), sum(a) FROM {t}", "7 30")

    # Explicit compaction keeps the data and merges the blocks.
    ok(f"OPTIMIZE TABLE {t} COMPACT")
    query("II", f"SELECT count(*), sum(a) FROM {t}", "7 30")
    query("I", f"SELECT count(*) FROM fuse_block('default', '{t}')", "1")

    # Auto compaction after write: three single-row inserts over the threshold
    # must leave fewer blocks than were appended.
    ok("SET auto_compaction_imperfect_blocks_threshold = 2")
    ok(f"INSERT INTO {t} VALUES (9, 'i')")
    ok(f"INSERT INTO {t} VALUES (10, 'j')")
    ok(f"INSERT INTO {t} VALUES (11, 'k')")
    ok("UNSET auto_compaction_imperfect_blocks_threshold")
    query("II", f"SELECT count(*), sum(a) FROM {t}", "10 60")
    query("B", f"SELECT count(*) < 4 FROM fuse_block('default', '{t}')", "1")

    # Explicit transaction with several statements on the table, then rollback.
    ok("BEGIN")
    ok(f"INSERT INTO {t} VALUES (12, 'l')")
    ok(f"DELETE FROM {t} WHERE a = 12")
    ok(f"INSERT INTO {t} VALUES (13, 'm')")
    ok(f"UPDATE {t} SET b = 'M' WHERE a = 13")
    ok("COMMIT")
    query("II", f"SELECT count(*), sum(a) FROM {t}", "11 73")
    query("T", f"SELECT b FROM {t} WHERE a = 13", "M")
    ok("BEGIN")
    ok(f"INSERT INTO {t} VALUES (14, 'n')")
    ok("ROLLBACK")
    query("II", f"SELECT count(*), sum(a) FROM {t}", "11 73")

    # Overwrite and truncate.
    ok(f"INSERT OVERWRITE {t} VALUES (100, 'z')")
    query("II", f"SELECT count(*), sum(a) FROM {t}", "1 100")
    ok(f"TRUNCATE TABLE {t}")
    query("I", f"SELECT count(*) FROM {t}", "0")
    ok(f"INSERT INTO {t} VALUES (1, 'a')")
    query("I", f"SELECT count(*) FROM {t}", "1")

    # CREATE ... AS SELECT of the same kind.
    ok(f"CREATE OR REPLACE {kw}TABLE {t}_ctas AS SELECT * FROM {t}")
    query("I", f"SELECT count(*) FROM {t}_ctas", "1")

    # Kind-specific semantics.
    #
    # Snapshot history: after the writes above a regular or temp table has a chain of
    # snapshots; a transient table keeps exactly one, and a session retention setting
    # that asks for more history must not change that.
    ok("SET data_retention_num_snapshots_to_keep = 20")
    ok(f"INSERT INTO {t} VALUES (2, 'b')")
    ok(f"INSERT INTO {t} VALUES (3, 'c')")
    ok("UNSET data_retention_num_snapshots_to_keep")
    query(
        "B",
        f"SELECT count(*) {'>' if kind.keeps_history else '='} 1 FROM fuse_snapshot('default', '{t}')",
        "1",
    )
    # Streams (only a regular table can carry one) need an enterprise license and are
    # covered by the `ee` suite, so they are not part of this matrix.

    ok(f"DROP TABLE {t}_ctas")
    ok(f"DROP TABLE {t}")
    ok(f"DROP TABLE {src}")


def main():
    out: list[str] = []
    out.append(
        "# Generated by tests/sqllogictests/scripts/gen_table_kind_matrix.py. Do not edit by"
    )
    out.append("# hand; change the generator and rerun it.")
    out.append("#")
    out.append(
        "# The same sequence of write operations against every kind of fuse table, with the"
    )
    out.append("# same expected results, so a kind-specific divergence shows up as a diff.")
    out.append("")
    for kind in KINDS:
        gen_kind(out, kind)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text("\n".join(out).rstrip() + "\n")
    print(f"wrote {OUTPUT.relative_to(Path(os.getcwd()))}")


if __name__ == "__main__":
    main()
