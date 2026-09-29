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

"""Generate the type x nullability x keyed-operator matrix sqllogictest.

Operators that take a *key* (sort, window ORDER BY / PARTITION BY, GROUP BY,
DISTINCT, JOIN ON, IN) dispatch on the key's data type and usually have a
fast path per type family plus a generic fallback. Historically the Nullable
wrapper and the less common types (Decimal256, Interval, Variant, Tuple, ...)
were only covered where someone happened to write a test, and bugs such as
"window ORDER BY nullable Timestamp panics" surfaced one type at a time.

This script writes one test file that runs every keyed operator over every
key type, both NOT NULL and NULL, and over four data shapes (no NULLs, mixed
NULLs, all NULLs, empty). Expected results are derived from how the data is
constructed, so they do not depend on the type and no oracle is needed.

Regenerate with:

    python3 tests/sqllogictests/scripts/gen_type_matrix.py

and commit the resulting `.test` file together with this script.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

OUTPUT = (
    Path(__file__).resolve().parents[1]
    / "suites"
    / "query"
    / "type_matrix"
    / "keyed_operators.test"
)

# Rows per table and distinct non-null keys. NULLS_EVERY controls the mixed
# shape: row `i` is NULL when `i % NULLS_EVERY == NULLS_EVERY - 1`.
ROWS = 60
KEYS = 6
NULLS_EVERY = 5


@dataclass(frozen=True)
class KeyType:
    name: str
    sql_type: str
    # SQL expression producing a distinct key for each distinct value of `{x}`
    # (an unsigned integer expression).
    gen: str
    keys: int = KEYS
    # `eq` is not defined for the type, so it cannot be a join / IN key.
    no_eq: bool = False
    # The row-format sort key encoding does not support the type yet
    # ("Row format is not yet support for RowSortField"), so ORDER BY and
    # window ORDER BY / PARTITION BY are skipped. Remove the flag once the
    # encoding supports the type; the matrix then covers it automatically.
    no_sort: bool = False


TYPES = [
    KeyType("boolean", "BOOLEAN", "({x} = 0)", keys=2),
    KeyType("int8", "INT8", "{x}::INT8"),
    KeyType("int16", "INT16", "{x}::INT16"),
    KeyType("int32", "INT32", "{x}::INT32"),
    KeyType("int64", "INT64", "{x}::INT64"),
    KeyType("uint8", "UINT8", "{x}::UINT8"),
    KeyType("uint16", "UINT16", "{x}::UINT16"),
    KeyType("uint32", "UINT32", "{x}::UINT32"),
    KeyType("uint64", "UINT64", "{x}::UINT64"),
    KeyType("float32", "FLOAT32", "{x}::FLOAT32 / 4"),
    KeyType("float64", "FLOAT64", "{x}::FLOAT64 / 4"),
    KeyType("decimal64", "DECIMAL(15, 2)", "{x}::DECIMAL(15, 2) / 4"),
    KeyType("decimal128", "DECIMAL(38, 6)", "{x}::DECIMAL(38, 6) / 4"),
    KeyType("decimal256", "DECIMAL(76, 10)", "{x}::DECIMAL(76, 10) / 4"),
    KeyType("string", "STRING", "'k' || to_string({x})"),
    KeyType("binary", "BINARY", "to_binary('k' || to_string({x}))", no_eq=True),
    KeyType("date", "DATE", "to_date({x}::INT32)"),
    KeyType("timestamp", "TIMESTAMP", "to_timestamp({x}::INT64)"),
    KeyType("interval", "INTERVAL", "to_interval(to_string({x}) || ' day')"),
    KeyType("variant", "VARIANT", "parse_json(to_string({x}))"),
    KeyType("array_int32", "ARRAY(INT32)", "[{x}::INT32]", no_sort=True),
    KeyType(
        "tuple",
        "TUPLE(INT32, STRING)",
        "({x}::INT32, 'k' || to_string({x}))",
        no_eq=True,
        no_sort=True,
    ),
]


@dataclass(frozen=True)
class Shape:
    name: str
    nullable: bool
    rows: int
    # None for a NOT NULL column or a shape without NULLs; a callable
    # otherwise deciding whether row `i` is NULL.
    null_at: object = None


def mixed(i: int) -> bool:
    return i % NULLS_EVERY == NULLS_EVERY - 1


def always(_: int) -> bool:
    return True


SHAPES = [
    Shape("dense_nn", nullable=False, rows=ROWS),
    Shape("dense", nullable=True, rows=ROWS),
    Shape("mixed", nullable=True, rows=ROWS, null_at=mixed),
    Shape("all_null", nullable=True, rows=ROWS, null_at=always),
    Shape("empty", nullable=True, rows=0),
]


class Model:
    """Reference model of one table: row -> key index or None."""

    def __init__(self, ty: KeyType, shape: Shape):
        self.rows = shape.rows
        self.keys = []
        for i in range(shape.rows):
            if shape.null_at is not None and shape.null_at(i):
                self.keys.append(None)
            else:
                self.keys.append(i % ty.keys)
        self.nulls = sum(1 for k in self.keys if k is None)
        self.counts: dict[int, int] = {}
        for k in self.keys:
            if k is not None:
                self.counts[k] = self.counts.get(k, 0) + 1
        self.distinct = len(self.counts)

    @property
    def groups(self) -> int:
        """Groups produced by GROUP BY / DISTINCT (NULL forms one group)."""
        return self.distinct + (1 if self.nulls else 0)

    @property
    def self_join(self) -> int:
        return sum(c * c for c in self.counts.values())

    def in_first(self, n: int) -> int:
        return sum(self.counts.get(k, 0) for k in range(n))


def key_expr(ty: KeyType, shape: Shape) -> str:
    value = ty.gen.format(x=f"(number % {ty.keys})")
    if shape.null_at is mixed:
        return f"IF(number % {NULLS_EVERY} = {NULLS_EVERY - 1}, NULL, {value})"
    if shape.null_at is always:
        return f"IF(true, NULL, {value})"
    return value


def emit(out: list[str], header: str, sql: str, columns: str, rows: list[str]):
    out.append(f"query {columns}")
    out.append(sql)
    out.append("----")
    out.extend(rows)
    out.append("")


def bool_(v: bool) -> str:
    return "1" if v else "0"


def gen_case(out: list[str], ty: KeyType, shape: Shape):
    table = f"tm_{ty.name}_{shape.name}"
    model = Model(ty, shape)
    nullability = "NULL" if shape.nullable else "NOT NULL"
    n = model.rows

    out.append(f"## {ty.sql_type} {nullability}, shape {shape.name}")
    out.append("statement ok")
    out.append(f"CREATE OR REPLACE TABLE {table}(k {ty.sql_type} {nullability})")
    out.append("")
    if n > 0:
        out.append("statement ok")
        out.append(
            f"INSERT INTO {table} SELECT {key_expr(ty, shape)} FROM numbers({n})"
        )
        out.append("")

    if not ty.no_sort:
        gen_sort_cases(out, table, model)
    gen_aggregate_cases(out, table, model)
    if not ty.no_eq:
        gen_eq_cases(out, ty, table, model)
    if not ty.no_sort:
        gen_window_cases(out, table, model)

    out.append("statement ok")
    out.append(f"DROP TABLE {table}")
    out.append("")


def gen_sort_cases(out: list[str], table: str, model: Model):
    """Full sort and top-n, NULL keys in both directions."""
    n = model.rows
    emit(
        out,
        "sort",
        f"SELECT count(*), count(k) FROM (SELECT k FROM {table} ORDER BY k NULLS FIRST)",
        "II",
        [f"{n} {n - model.nulls}"],
    )
    emit(
        out,
        "top_n",
        f"SELECT count(*) FROM (SELECT k FROM {table} ORDER BY k DESC NULLS LAST LIMIT 3)",
        "I",
        [str(min(n, 3))],
    )


def gen_aggregate_cases(out: list[str], table: str, model: Model):
    """GROUP BY / DISTINCT keys and min/max over the key type."""
    n = model.rows
    emit(
        out,
        "group_by",
        f"SELECT count(*), sum(c) FROM (SELECT k, count(*) c FROM {table} GROUP BY k)",
        "II",
        [f"{model.groups} {n if n else 'NULL'}"],
    )
    emit(
        out,
        "distinct",
        f"SELECT count(*) FROM (SELECT DISTINCT k FROM {table})",
        "I",
        [str(model.groups)],
    )
    emit(
        out,
        "count_distinct",
        f"SELECT count(DISTINCT k) FROM {table}",
        "I",
        [str(model.distinct)],
    )
    # `count(k)` is folded from column statistics when they are exact.
    emit(
        out,
        "count_column",
        f"SELECT count(k), count(*) FROM {table}",
        "II",
        [f"{n - model.nulls} {n}"],
    )
    emit(
        out,
        "min_max",
        f"SELECT min(k) IS NOT NULL, max(k) IS NOT NULL FROM {table}",
        "BB",
        [f"{bool_(model.distinct > 0)} {bool_(model.distinct > 0)}"],
    )


def gen_eq_cases(out: list[str], ty: KeyType, table: str, model: Model):
    """Join keys and IN: NULL never matches, so inner = sum(c^2), left adds NULL rows."""
    emit(
        out,
        "inner_join",
        f"SELECT count(*) FROM {table} a JOIN {table} b ON a.k = b.k",
        "I",
        [str(model.self_join)],
    )
    emit(
        out,
        "left_join",
        f"SELECT count(*) FROM {table} a LEFT JOIN {table} b ON a.k = b.k",
        "I",
        [str(model.self_join + model.nulls)],
    )
    emit(
        out,
        "in_subquery",
        f"SELECT count(*) FROM {table} WHERE k IN (SELECT {ty.gen.format(x='number')} FROM numbers(2))",
        "I",
        [str(model.in_first(2))],
    )


def gen_window_cases(out: list[str], table: str, model: Model):
    """Window PARTITION BY / ORDER BY keys; the running sum is order independent."""
    n = model.rows
    max_partition = max([model.nulls, *model.counts.values()], default=0)
    emit(
        out,
        "window_partition",
        f"SELECT count(*), max(rn) FROM (SELECT row_number() OVER (PARTITION BY k ORDER BY k) rn FROM {table})",
        "II",
        [f"{n} {max_partition if n else 'NULL'}"],
    )
    emit(
        out,
        "window_order",
        f"SELECT sum(s) FROM (SELECT sum(1) OVER (ORDER BY k ROWS UNBOUNDED PRECEDING) s FROM {table})",
        "I",
        [str(n * (n + 1) // 2) if n else "NULL"],
    )


def main():
    out: list[str] = []
    out.append(
        "# Generated by tests/sqllogictests/scripts/gen_type_matrix.py. Do not edit by hand;"
    )
    out.append("# change the generator and rerun it.")
    out.append("#")
    out.append(
        "# Every keyed operator (sort, top-n, group by, distinct, join, in, window) over"
    )
    out.append(
        "# every key type, NOT NULL and NULL, and four data shapes. Expected values follow"
    )
    out.append(
        "# from the data construction (row count, distinct keys, per-key counts) and are"
    )
    out.append("# therefore identical for every type.")
    out.append("#")
    out.append("# The same file is rerun on the legacy sort and join paths.")
    out.append("# run-with-settings: enable_fixed_rows_sort=0")
    out.append("# run-with-settings: enable_experimental_new_join=0")
    out.append("")
    for ty in TYPES:
        for shape in SHAPES:
            gen_case(out, ty, shape)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text("\n".join(out).rstrip() + "\n")
    print(f"wrote {OUTPUT.relative_to(Path(os.getcwd()))}")


if __name__ == "__main__":
    main()
