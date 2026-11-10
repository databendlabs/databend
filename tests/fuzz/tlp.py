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

"""Metamorphic query testing with self-checking oracles (no reference engine).

Grammar-based fuzzing only catches crashes and errors; the bugs that hurt most
return a wrong result silently (a LEFT JOIN turned into an INNER JOIN, a row
dropped by pruning, a NULL treated as a value). This harness generates random
predicates over small tables and checks algebraic identities that must hold for
*every* predicate, so a mismatch is a bug without needing another database to
compare against:

- TLP (ternary logic partitioning, Rigger & Su 2020): for any predicate p,
      Q                ==  Q WHERE p  UNION ALL  Q WHERE NOT p  UNION ALL  Q WHERE p IS NULL
  compared as multisets of rows. Exercises filter push-down, join rewrites and
  NULL semantics.
- NoREC (non-optimizing reference): for any predicate p,
      SELECT count(*) FROM T WHERE p  ==  SELECT sum(CASE WHEN p THEN 1 ELSE 0 END) FROM T
  The right-hand side evaluates p as a projection, bypassing filter push-down,
  index pruning and prewhere, so a disagreement points at the filtering path.
- Join TLP: INNER JOIN ON p == CROSS JOIN WHERE p, and TLP over the ON predicate
  for LEFT JOIN row counts.
- Aggregation consistency: DISTINCT vs GROUP BY, count(DISTINCT) vs GROUP BY.

Every case is deterministic given the seed. On a mismatch the script prints the
seed and the exact SQL statements to reproduce, then exits non-zero.

    python3 tests/fuzz/tlp.py --iterations 300 --seed 42

Only internal errors (Internal, PanicError, UnwindError, StorageNotFound) and
inconsistencies fail the run. A generated query that the server rejects with a
semantic/type error is skipped: the generator is deliberately loose about types
so that type coercion paths are exercised too.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass

# Error codes that always indicate a bug.
FATAL_ERROR_CODES = {
    1001,  # Internal
    1104,  # PanicError / UnwindError
    3001,  # StorageNotFound
}


class Client:
    def __init__(self, host: str, port: int, user: str, database: str):
        self.url = f"http://{host}:{port}/v1/query"
        self.user = user
        self.database = database

    def query(self, sql: str) -> tuple[list[list], dict | None]:
        body = json.dumps(
            {"sql": sql, "session": {"database": self.database}, "pagination": {"wait_time_secs": 30}}
        ).encode()
        req = urllib.request.Request(self.url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", "Basic " + __import__("base64").b64encode(f"{self.user}:".encode()).decode())
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                result = json.load(resp)
        except urllib.error.URLError as err:
            raise ConnectionError(f"request failed: {err}") from err
        if result.get("error"):
            return [], result["error"]
        rows = list(result.get("data", []))
        while result.get("next_uri"):
            next_url = f"{self.url.rsplit('/v1/', 1)[0]}{result['next_uri']}"
            req = urllib.request.Request(next_url, method="GET")
            req.add_header("Authorization", "Basic " + __import__("base64").b64encode(f"{self.user}:".encode()).decode())
            with urllib.request.urlopen(req, timeout=120) as resp:
                result = json.load(resp)
            if result.get("error"):
                return [], result["error"]
            rows.extend(result.get("data", []))
        return rows, None

    def execute(self, sql: str) -> None:
        _, err = self.query(sql)
        if err:
            raise RuntimeError(f"{sql}\n  -> {err}")


@dataclass
class Column:
    name: str
    kind: str  # int | str | date | dec | bool


TABLES = {
    "tlp_t1": [
        Column("a", "int"),
        Column("b", "int"),
        Column("c", "str"),
        Column("d", "date"),
        Column("e", "dec"),
        Column("f", "bool"),
    ],
    "tlp_t2": [
        Column("a", "int"),
        Column("x", "int"),
        Column("c", "str"),
    ],
}

SQL_TYPES = {
    "int": "INT NULL",
    "str": "STRING NULL",
    "date": "DATE NULL",
    "dec": "DECIMAL(10, 2) NULL",
    "bool": "BOOLEAN NULL",
}

STRINGS = ["a", "b", "ab", "abc", "", "A", "Z", "hello", "héllo", "x y"]
DATES = ["2024-01-01", "2024-02-29", "2023-12-31", "1970-01-01", "2030-06-15"]


def literal(rng: random.Random, kind: str, allow_null: bool = True) -> str:
    if allow_null and rng.random() < 0.1:
        return "NULL"
    if kind == "int":
        # Keep integers small: nested arithmetic on extreme values overflows Int64, which
        # wraps by design (and panics in debug builds), and would drown real findings.
        return str(rng.choice([0, 1, -1, 2, 3, 7, 10, 100, -100, 1000]))
    if kind == "str":
        return "'" + rng.choice(STRINGS).replace("'", "''") + "'"
    if kind == "date":
        return f"'{rng.choice(DATES)}'::DATE"
    if kind == "dec":
        return rng.choice(["0", "1.5", "-2.25", "100.01", "0.01", "99999999.99"])
    if kind == "bool":
        return rng.choice(["TRUE", "FALSE"])
    raise AssertionError(kind)


def value_for_insert(rng: random.Random, kind: str) -> str:
    if rng.random() < 0.25:
        return "NULL"
    return literal(rng, kind, allow_null=False)


class Generator:
    """Random predicates over the columns of one or two tables."""

    def __init__(self, rng: random.Random, columns: dict[str, list[tuple[str, Column]]]):
        self.rng = rng
        # alias -> [(qualified name, column)]
        self.columns = columns

    def cols(self, kind: str) -> list[str]:
        return [name for cols in self.columns.values() for name, col in cols if col.kind == kind]

    def int_expr(self, depth: int) -> str:
        r = self.rng
        ints = self.cols("int")
        choices = ["col", "col", "lit"]
        if depth > 0:
            choices += ["arith", "unary", "func", "case"]
        pick = r.choice(choices)
        if pick == "col" and ints:
            return r.choice(ints)
        if pick == "lit" or pick == "col":
            return literal(r, "int")
        if pick == "arith":
            op = r.choice(["+", "-", "*"])
            return f"({self.int_expr(depth - 1)} {op} {self.int_expr(depth - 1)})"
        if pick == "unary":
            f = r.choice(["-", "abs", "sign"])
            inner = self.int_expr(depth - 1)
            # Parenthesise the operand: `--1` would start a comment.
            return f"(-({inner}))" if f == "-" else f"{f}({inner})"
        if pick == "func":
            f = r.choice(["coalesce", "if", "greatest", "least"])
            if f == "coalesce":
                return f"coalesce({self.int_expr(depth - 1)}, {self.int_expr(depth - 1)})"
            if f == "if":
                return f"if({self.predicate(depth - 1)}, {self.int_expr(depth - 1)}, {self.int_expr(depth - 1)})"
            return f"{f}({self.int_expr(depth - 1)}, {self.int_expr(depth - 1)})"
        return (
            f"(CASE WHEN {self.predicate(depth - 1)} THEN {self.int_expr(depth - 1)} "
            f"ELSE {self.int_expr(depth - 1)} END)"
        )

    def str_expr(self, depth: int) -> str:
        r = self.rng
        strs = self.cols("str")
        pick = r.choice(["col", "col", "lit", "func"] if depth > 0 else ["col", "lit"])
        if pick == "col" and strs:
            return r.choice(strs)
        if pick in ("lit", "col"):
            return literal(r, "str")
        f = r.choice(["lower", "upper", "concat", "coalesce", "substr"])
        if f in ("lower", "upper"):
            return f"{f}({self.str_expr(depth - 1)})"
        if f == "substr":
            return f"substr({self.str_expr(depth - 1)}, 1, 2)"
        return f"{f}({self.str_expr(depth - 1)}, {self.str_expr(depth - 1)})"

    def predicate(self, depth: int) -> str:
        r = self.rng
        kinds = ["cmp_int", "cmp_int", "cmp_str", "is_null", "in_list", "between", "bool_col", "like"]
        if self.cols("date"):
            kinds.append("cmp_date")
        if self.cols("dec"):
            kinds.append("cmp_dec")
        if depth > 0:
            kinds += ["and", "or", "not", "and", "or"]
        pick = r.choice(kinds)
        cmp = r.choice(["=", "<>", "<", "<=", ">", ">="])
        if pick == "cmp_int":
            return f"{self.int_expr(depth - 1)} {cmp} {self.int_expr(depth - 1)}"
        if pick == "cmp_str":
            return f"{self.str_expr(depth - 1)} {cmp} {self.str_expr(depth - 1)}"
        if pick == "cmp_date":
            return f"{r.choice(self.cols('date'))} {cmp} {literal(r, 'date')}"
        if pick == "cmp_dec":
            rhs = r.choice([literal(r, "dec"), self.int_expr(depth - 1)])
            return f"{r.choice(self.cols('dec'))} {cmp} {rhs}"
        if pick == "is_null":
            col = r.choice([c for cols in self.columns.values() for c, _ in cols])
            return f"{col} IS {r.choice(['', 'NOT '])}NULL"
        if pick == "in_list":
            items = ", ".join(literal(r, "int") for _ in range(r.randint(1, 4)))
            return f"{self.int_expr(depth - 1)} {r.choice(['', 'NOT '])}IN ({items})"
        if pick == "between":
            return f"{self.int_expr(depth - 1)} BETWEEN {literal(r, 'int')} AND {literal(r, 'int')}"
        if pick == "bool_col":
            bools = self.cols("bool")
            return r.choice(bools) if bools else f"{self.int_expr(depth - 1)} > 0"
        if pick == "like":
            strs = self.cols("str")
            if not strs:
                return self.predicate(depth)
            pattern = r.choice(["'a%'", "'%b%'", "'_'", "'%'", "''", "'h_llo'"])
            return f"{r.choice(strs)} {r.choice(['', 'NOT '])}LIKE {pattern}"
        if pick == "and":
            return f"({self.predicate(depth - 1)} AND {self.predicate(depth - 1)})"
        if pick == "or":
            return f"({self.predicate(depth - 1)} OR {self.predicate(depth - 1)})"
        return f"NOT ({self.predicate(depth - 1)})"


class Failure(Exception):
    pass


class Harness:
    def __init__(self, client: Client, rng: random.Random, verbose: bool):
        self.client = client
        self.rng = rng
        self.verbose = verbose
        self.checked = 0
        self.skipped = 0

    # -- setup -----------------------------------------------------------------

    def setup(self, rows: int):
        for table, columns in TABLES.items():
            cols = ", ".join(f"{c.name} {SQL_TYPES[c.kind]}" for c in columns)
            self.client.execute(f"CREATE OR REPLACE TABLE {table}({cols})")
            values = []
            for _ in range(rows):
                values.append("(" + ", ".join(value_for_insert(self.rng, c.kind) for c in columns) + ")")
            # Several inserts so the table has several blocks, so pruning has work to do.
            for i in range(0, len(values), 8):
                self.client.execute(f"INSERT INTO {table} VALUES {', '.join(values[i:i + 8])}")
        # A clustered copy exercises range/cluster pruning with the same data.
        self.client.execute("CREATE OR REPLACE TABLE tlp_t1c CLUSTER BY (a, d) AS SELECT * FROM tlp_t1")
        self.client.execute("OPTIMIZE TABLE tlp_t1c COMPACT")

    # -- helpers ---------------------------------------------------------------

    def run(self, sql: str, case: list[str]) -> list[list] | None:
        rows, err = self.client.query(sql)
        if err is None:
            return rows
        code = err.get("code")
        message = err.get("message", "")
        if code in FATAL_ERROR_CODES or "panic" in message.lower():
            raise Failure(f"fatal error {code}: {message}\n" + self.repro(case + [sql]))
        return None

    def repro(self, statements: list[str]) -> str:
        return "reproduce:\n" + "\n".join(f"  {s};" for s in statements)

    def check_multiset(self, base_sql: str, part_sqls: list[str], case: list[str]):
        base = self.run(base_sql, case)
        if base is None:
            self.skipped += 1
            return
        union: Counter = Counter()
        for sql in part_sqls:
            rows = self.run(sql, case)
            if rows is None:
                raise Failure(
                    f"partition query failed while the base query succeeded\n"
                    + self.repro(case + [base_sql, sql])
                )
            union.update(tuple(r) for r in rows)
        expected = Counter(tuple(r) for r in base)
        if union != expected:
            missing = expected - union
            extra = union - expected
            raise Failure(
                "TLP multiset mismatch\n"
                f"  base rows: {sum(expected.values())}, partition rows: {sum(union.values())}\n"
                f"  missing from partitions: {list(missing.elements())[:5]}\n"
                f"  extra in partitions: {list(extra.elements())[:5]}\n"
                + self.repro(case + [base_sql, *part_sqls])
            )
        self.checked += 1

    def check_scalar_equal(self, lhs_sql: str, rhs_sql: str, case: list[str]):
        lhs = self.run(lhs_sql, case)
        if lhs is None:
            self.skipped += 1
            return
        rhs = self.run(rhs_sql, case)
        if rhs is None:
            raise Failure("reference query failed while the query succeeded\n" + self.repro(case + [lhs_sql, rhs_sql]))
        if lhs != rhs:
            raise Failure(f"result mismatch: {lhs} vs {rhs}\n" + self.repro(case + [lhs_sql, rhs_sql]))
        self.checked += 1

    # -- oracles ---------------------------------------------------------------

    def tlp_where(self, table: str):
        gen = Generator(self.rng, {"t": [(c.name, c) for c in TABLES["tlp_t1" if table == "tlp_t1c" else table]]})
        p = gen.predicate(3)
        proj = "*" if self.rng.random() < 0.7 else gen.int_expr(2)
        base = f"SELECT {proj} FROM {table}"
        parts = [f"{base} WHERE {p}", f"{base} WHERE NOT ({p})", f"{base} WHERE ({p}) IS NULL"]
        self.check_multiset(base, parts, [])

    def norec(self, table: str):
        gen = Generator(self.rng, {"t": [(c.name, c) for c in TABLES["tlp_t1" if table == "tlp_t1c" else table]]})
        p = gen.predicate(3)
        self.check_scalar_equal(
            f"SELECT count(*) FROM {table} WHERE {p}",
            f"SELECT coalesce(sum(CASE WHEN {p} THEN 1 ELSE 0 END), 0) FROM {table}",
            [],
        )

    def tlp_join(self):
        cols = {
            "l": [(f"l.{c.name}", c) for c in TABLES["tlp_t1"]],
            "r": [(f"r.{c.name}", c) for c in TABLES["tlp_t2"]],
        }
        gen = Generator(self.rng, cols)
        p = gen.predicate(2)
        # INNER JOIN ON p is a CROSS JOIN filtered by p.
        self.check_multiset(
            f"SELECT l.a, l.c, r.a, r.x FROM tlp_t1 l CROSS JOIN tlp_t2 r WHERE {p}",
            [f"SELECT l.a, l.c, r.a, r.x FROM tlp_t1 l INNER JOIN tlp_t2 r ON {p}"],
            [],
        )
        # TLP over a WHERE predicate on top of a LEFT JOIN (null-extended rows included).
        q = gen.predicate(2)
        base = f"SELECT l.a, l.c, r.a, r.x FROM tlp_t1 l LEFT JOIN tlp_t2 r ON l.a = r.a"
        self.check_multiset(
            base,
            [f"{base} WHERE {q}", f"{base} WHERE NOT ({q})", f"{base} WHERE ({q}) IS NULL"],
            [],
        )

    def tlp_aggregate(self, table: str):
        gen = Generator(self.rng, {"t": [(c.name, c) for c in TABLES["tlp_t1" if table == "tlp_t1c" else table]]})
        p = gen.predicate(2)
        key = self.rng.choice([c.name for c in TABLES["tlp_t1" if table == "tlp_t1c" else table]])
        # count(*) partitions add up.
        self.check_scalar_equal(
            f"SELECT count(*) FROM {table}",
            f"SELECT (SELECT count(*) FROM {table} WHERE {p}) + (SELECT count(*) FROM {table} WHERE NOT ({p})) "
            f"+ (SELECT count(*) FROM {table} WHERE ({p}) IS NULL)",
            [],
        )
        # DISTINCT and GROUP BY agree on the number of groups.
        self.check_scalar_equal(
            f"SELECT count(*) FROM (SELECT DISTINCT {key} FROM {table} WHERE {p})",
            f"SELECT count(*) FROM (SELECT {key} FROM {table} WHERE {p} GROUP BY {key})",
            [],
        )

    def iteration(self):
        table = self.rng.choice(["tlp_t1", "tlp_t1", "tlp_t1c", "tlp_t2"])
        oracle = self.rng.choice(["tlp_where", "tlp_where", "norec", "tlp_join", "tlp_aggregate"])
        if oracle == "tlp_where":
            self.tlp_where(table)
        elif oracle == "norec":
            self.norec(table)
        elif oracle == "tlp_join":
            self.tlp_join()
        else:
            self.tlp_aggregate(table)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--iterations", type=int, default=int(os.getenv("TLP_ITERATIONS", "200")))
    parser.add_argument("--seed", type=int, default=int(os.getenv("TLP_SEED", str(random.randrange(1 << 31)))))
    parser.add_argument("--rows", type=int, default=40)
    parser.add_argument("--host", default=os.getenv("QUERY_HTTP_HANDLER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("QUERY_HTTP_HANDLER_PORT", "8000")))
    parser.add_argument("--user", default=os.getenv("QUERY_USER", "root"))
    parser.add_argument("--database", default="default")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    print(f"tlp: seed={args.seed} iterations={args.iterations} rows={args.rows}")
    rng = random.Random(args.seed)
    client = Client(args.host, args.port, args.user, args.database)
    harness = Harness(client, rng, args.verbose)
    harness.setup(args.rows)

    for i in range(args.iterations):
        try:
            harness.iteration()
        except Failure as failure:
            print(f"tlp: FAILED at iteration {i} (seed={args.seed})")
            print(failure)
            return 1
    print(f"tlp: OK, {harness.checked} checks passed, {harness.skipped} skipped (rejected by the server)")
    if harness.checked == 0:
        print("tlp: nothing was checked, generator or server misconfigured")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
