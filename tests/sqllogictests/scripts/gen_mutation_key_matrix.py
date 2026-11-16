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

"""Generate a shared MERGE/REPLACE key-type test and a cluster wrapper.

Regenerate both files with:

    python3 tests/sqllogictests/scripts/gen_mutation_key_matrix.py

The expected *rows* are specified independently of key representation, not
inferred by querying Databend. SQL equality does not match NULL to NULL, and
REPLACE INTO deliberately retains rows whose conflict key is NULL (see
09_0024_replace_into_decimal_key.test). We assert each operation's outcome
separately rather than assuming MERGE and REPLACE have identical semantics.
"""

from dataclasses import dataclass
from pathlib import Path

SUITES = Path(__file__).resolve().parents[1] / "suites"
OUTPUTS = {
    "local": SUITES / "base" / "09_fuse_engine" / "09_0061_mutation_key_matrix.test",
    "cluster": SUITES / "mode" / "cluster" / "mutation_key_matrix.test",
}


@dataclass(frozen=True)
class Key:
    name: str
    data_type: str
    values: tuple[str, str, str, str]


KEYS = (
    Key("int", "INT", ("1", "2", "3", "4")),
    Key(
        "decimal128",
        "DECIMAL(38, 6)",
        ("1.000001", "2.000002", "3.000003", "4.000004"),
    ),
    Key(
        "date",
        "DATE",
        ("'2026-01-01'", "'2026-01-02'", "'2026-01-03'", "'2026-01-04'"),
    ),
    Key(
        "timestamp",
        "TIMESTAMP",
        (
            "'2026-01-01 00:00:01'",
            "'2026-01-01 00:00:02'",
            "'2026-01-01 00:00:03'",
            "'2026-01-01 00:00:04'",
        ),
    ),
)


def statement(out: list[str], sql: str) -> None:
    out.extend(("statement ok", sql, ""))


def query(out: list[str], sql: str, rows: list[tuple[str, int]]) -> None:
    out.extend(("query TI", f"{sql} ORDER BY v", "----"))
    out.extend(f"{name} {is_null}" for name, is_null in sorted(rows))
    out.append("")


def mutation(out: list[str], op: str, table: str, source: str) -> None:
    if op == "replace":
        statement(out, f"REPLACE INTO {table} ON (k) SELECT k, v FROM {source}")
    else:
        statement(
            out,
            f"MERGE INTO {table} USING {source} AS s ON {table}.k = s.k "
            "WHEN MATCHED THEN UPDATE SET v = s.v "
            "WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)",
        )


def case(out: list[str], key: Key, nullable: bool, op: str) -> None:
    suffix = "nullable" if nullable else "not_null"
    table = f"mkm_{op}_{key.name}_{suffix}"
    source = f"{table}_src"
    k1, k2, k3, k4 = key.values
    constraint = "NULL" if nullable else "NOT NULL"
    out.append(f"# {op.upper()} / {key.data_type} / {constraint}")
    # Small blocks and two separate inserts exercise multi-block reads and pruning.
    statement(out, f"CREATE TABLE {table} (k {key.data_type} {constraint}, v STRING) ROW_PER_BLOCK = 2")
    statement(out, f"CREATE TABLE {source} (k {key.data_type} {constraint}, v STRING)")
    statement(out, f"INSERT INTO {table} VALUES ({k1}, 'old1'), ({k2}, 'old2')")
    statement(out, f"INSERT INTO {table} VALUES ({k4}, 'old4')")
    if nullable:
        statement(out, f"INSERT INTO {table} VALUES (NULL, 'null_old')")
    statement(out, f"INSERT INTO {source} VALUES ({k1}, 'new1'), ({k3}, 'new3')")
    if nullable:
        statement(out, f"INSERT INTO {source} VALUES (NULL, 'null_new')")
    mutation(out, op, table, source)

    rows = [("new1", 0), ("new3", 0), ("old2", 0), ("old4", 0)]
    if nullable:
        rows.extend((("null_old", 1), ("null_new", 1)))
    query(out, f"SELECT v, if(k IS NULL, 1, 0) FROM {table}", rows)

    # A second mutation re-matches the non-NULL key, but still does not
    # match the NULL inserted in the previous statement.
    statement(out, f"TRUNCATE TABLE {source}")
    statement(out, f"INSERT INTO {source} VALUES ({k1}, 'new1_again')")
    if nullable:
        statement(out, f"INSERT INTO {source} VALUES (NULL, 'null_again')")
    mutation(out, op, table, source)
    rows = [("new1_again", 0), ("new3", 0), ("old2", 0), ("old4", 0)]
    if nullable:
        rows.extend((("null_old", 1), ("null_new", 1), ("null_again", 1)))
    query(out, f"SELECT v, if(k IS NULL, 1, 0) FROM {table}", rows)

    if nullable:
        # All-NULL inputs cannot match on `=`. Both mutation operators must
        # retain *all* old rows and insert each new NULL row once.
        statement(out, f"TRUNCATE TABLE {table}")
        statement(out, f"TRUNCATE TABLE {source}")
        statement(out, f"INSERT INTO {table} VALUES (NULL, 'only_old1'), (NULL, 'only_old2')")
        statement(out, f"INSERT INTO {source} VALUES (NULL, 'only_new')")
        mutation(out, op, table, source)
        query(
            out,
            f"SELECT v, if(k IS NULL, 1, 0) FROM {table}",
            [("only_old1", 1), ("only_old2", 1), ("only_new", 1)],
        )

    statement(out, f"DROP TABLE {source}")
    statement(out, f"DROP TABLE {table}")


def generate_shared() -> str:
    out = [
        "# Generated by tests/sqllogictests/scripts/gen_mutation_key_matrix.py.",
        "# Do not edit by hand; change the generator and regenerate both files.",
        "# Row-set assertions cover matching, nonmatching and all-NULL keys,",
        "# including repeated mutations and multi-block targets.",
        "",
    ]
    statement(out, "SET timezone = 'UTC'")
    for key in KEYS:
        for op in ("replace", "merge"):
            case(out, key, False, op)
            case(out, key, True, op)
    statement(out, "UNSET timezone")
    return "\n".join(out).rstrip() + "\n"


def generate_cluster() -> str:
    out = [
        "# Generated by tests/sqllogictests/scripts/gen_mutation_key_matrix.py.",
        "# Exercise the shared matrix with distributed MERGE and REPLACE enabled.",
        "",
        "query I",
        "SELECT if(count(*) >= 2, 1, 0) FROM system.clusters",
        "----",
        "1",
        "",
    ]
    statement(out, "SET enable_distributed_replace_into = 1")
    statement(out, "SET enable_distributed_merge_into = 1")
    out.append("include ../../base/09_fuse_engine/09_0061_mutation_key_matrix.test")
    out.append("")
    statement(out, "UNSET enable_distributed_replace_into")
    statement(out, "UNSET enable_distributed_merge_into")
    return "\n".join(out).rstrip() + "\n"


def main() -> None:
    for mode, output in OUTPUTS.items():
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(generate_cluster() if mode == "cluster" else generate_shared())
        print(f"wrote {output}")


if __name__ == "__main__":
    main()
