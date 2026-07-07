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

"""Diagnose PROXY table routing against its physical target tables.

The script creates five FUSE tables with identical logical data and single,
two-column, and three-column cluster keys, then compares PROXY routing with the
scan costs reported by EXPLAIN for each predicate shape.
"""

import argparse
import base64
import http.client
import json
import math
import random
import re
import statistics
import sys
import time
from dataclasses import dataclass
from typing import Iterable


TABLE_PROXY = "proxy_bench_spans"
TABLE_TRACE = "proxy_bench_spans_by_trace"
TABLE_CHAT = "proxy_bench_spans_by_chat"
TABLE_USER = "proxy_bench_spans_by_user"
TABLE_TRACE_CHAT = "proxy_bench_spans_by_trace_chat"
TABLE_TRACE_CHAT_USER = "proxy_bench_spans_by_trace_chat_user"

MAX_ACCEPTABLE_RANK = 2
MIN_TOP1_HIT_RATIO = 0.85
STATISTICS_MIN_ACCEPTABLE_HIT_RATIO = 1.0
PREFIX_MIN_ACCEPTABLE_HIT_RATIO = 0.85


@dataclass(frozen=True)
class BenchCase:
    name: str
    predicates: tuple[str, ...]
    expected_count: int


@dataclass(frozen=True)
class ScanStats:
    partitions_scanned: int
    partitions_total: int
    read_rows: int
    read_bytes: int

    @property
    def cost(self) -> tuple[int, int]:
        return (self.partitions_scanned, self.read_rows)


@dataclass
class RouteResult:
    case_name: str
    best_route: str
    best_routes: list[str]
    actual_route: str
    expected_count: int
    actual_count: int
    stats_by_route: dict[str, ScanStats]
    query_ms_by_route: dict[str, float]
    route_rank: int | None
    top1_match: bool
    acceptable_route: bool
    passed: bool
    reason: str


@dataclass(frozen=True)
class QueryResult:
    rows: list[tuple]
    elapsed_ms: float


def quote_ident(name: str) -> str:
    return f"`{name}`"


def trace_id(value: int, width: int) -> str:
    return f"trace_{value:0{width}d}"


def chat_id(value: int, width: int) -> str:
    return f"chat_{value:0{width}d}"


def user_id(value: int, width: int) -> str:
    return f"user_{value:0{width}d}"


def count_mod_constraints(rows: int, constraints: list[tuple[int, int]]) -> int:
    residue = 0
    step = 1
    for modulus, expected in constraints:
        expected %= modulus
        gcd = math.gcd(step, modulus)
        if (expected - residue) % gcd != 0:
            return 0

        while residue % modulus != expected:
            residue += step
        step = math.lcm(step, modulus)

    if residue >= rows:
        return 0
    return ((rows - 1 - residue) // step) + 1


def count_mod_range(rows: int, cardinality: int, start: int, end: int) -> int:
    return sum(
        count_mod_constraints(rows, [(cardinality, value)])
        for value in range(start, end)
    )


def build_cases(
    rows: int,
    trace_cardinality: int,
    trace_width: int,
    chat_cardinality: int,
    chat_width: int,
    user_cardinality: int,
    user_width: int,
) -> list[BenchCase]:
    trace_point = min(trace_cardinality // 2, trace_cardinality - 1)
    trace_range_start = min(max(trace_cardinality // 3, 0), trace_cardinality - 11)
    trace_range_end = trace_range_start + 10

    chat_point = min(chat_cardinality // 2, chat_cardinality - 1)
    chat_range_start = min(max(chat_cardinality // 3, 0), chat_cardinality - 11)
    chat_range_end = chat_range_start + 10
    user_point = min(user_cardinality // 2, user_cardinality - 1)
    user_range_start = min(max(user_cardinality // 3, 0), user_cardinality - 11)
    user_range_end = user_range_start + 10
    composite_probe = max(rows // 2, 1)
    pair_trace_point = composite_probe % trace_cardinality
    pair_chat_point = composite_probe % chat_cardinality
    triple_trace_point = composite_probe % trace_cardinality
    triple_chat_point = composite_probe % chat_cardinality
    triple_user_point = composite_probe % user_cardinality

    return [
        BenchCase(
            "trace_point",
            (f"trace_id = '{trace_id(trace_point, trace_width)}'",),
            count_mod_constraints(rows, [(trace_cardinality, trace_point)]),
        ),
        BenchCase(
            "chat_point",
            (f"chat_id = '{chat_id(chat_point, chat_width)}'",),
            count_mod_constraints(rows, [(chat_cardinality, chat_point)]),
        ),
        BenchCase(
            "user_point",
            (f"user_id = '{user_id(user_point, user_width)}'",),
            count_mod_constraints(rows, [(user_cardinality, user_point)]),
        ),
        BenchCase(
            "trace_chat_point",
            (
                f"trace_id = '{trace_id(pair_trace_point, trace_width)}' "
                f"and chat_id = '{chat_id(pair_chat_point, chat_width)}'",
            ),
            count_mod_constraints(
                rows,
                [
                    (trace_cardinality, pair_trace_point),
                    (chat_cardinality, pair_chat_point),
                ],
            ),
        ),
        BenchCase(
            "trace_chat_user_point",
            (
                f"trace_id = '{trace_id(triple_trace_point, trace_width)}' "
                f"and chat_id = '{chat_id(triple_chat_point, chat_width)}' "
                f"and user_id = '{user_id(triple_user_point, user_width)}'",
            ),
            count_mod_constraints(
                rows,
                [
                    (trace_cardinality, triple_trace_point),
                    (chat_cardinality, triple_chat_point),
                    (user_cardinality, triple_user_point),
                ],
            ),
        ),
        BenchCase(
            "trace_range",
            (
                f"trace_id >= '{trace_id(trace_range_start, trace_width)}' "
                f"and trace_id < '{trace_id(trace_range_end, trace_width)}'",
            ),
            count_mod_range(rows, trace_cardinality, trace_range_start, trace_range_end),
        ),
        BenchCase(
            "chat_range",
            (
                f"chat_id >= '{chat_id(chat_range_start, chat_width)}' "
                f"and chat_id < '{chat_id(chat_range_end, chat_width)}'",
            ),
            count_mod_range(rows, chat_cardinality, chat_range_start, chat_range_end),
        ),
        BenchCase(
            "user_range",
            (
                f"user_id >= '{user_id(user_range_start, user_width)}' "
                f"and user_id < '{user_id(user_range_end, user_width)}'",
            ),
            count_mod_range(rows, user_cardinality, user_range_start, user_range_end),
        ),
    ]


class Databend:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.conn = http.client.HTTPConnection(
            args.host,
            args.port,
            timeout=args.connect_timeout,
        )
        token = base64.b64encode(f"{args.user}:{args.password}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
        }
        self.settings = {
            "enable_query_result_cache": "0",
            "enable_planner_cache": "0",
        }
        if args.disable_distributed_pruning:
            self.settings["enable_distributed_pruning"] = "0"
        if args.proxy_routing_model:
            self.settings["proxy_routing_model"] = args.proxy_routing_model
        if args.enable_proxy_bloom_pruning:
            self.settings["enable_proxy_bloom_pruning"] = "1"

    def close(self) -> None:
        self.conn.close()

    def request_json(self, method: str, path: str, payload: dict | None = None) -> dict:
        body = json.dumps(payload) if payload is not None else None
        self.conn.request(
            method,
            path,
            body=body,
            headers=self.headers,
        )
        response = self.conn.getresponse()
        response_body = response.read().decode()
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {response_body}")
        data = json.loads(response_body)
        if data.get("error"):
            raise RuntimeError(f"query failed: {data['error']}")
        return data

    def execute(self, sql: str) -> list[tuple]:
        payload = {
            "sql": sql,
            "session": {
                "database": self.args.database,
                "settings": self.settings,
            },
            "pagination": {"wait_time_secs": 60},
        }
        data = self.request_json("POST", "/v1/query", payload)
        deadline = time.monotonic() + self.args.connect_timeout
        while data.get("state") == "Running":
            if time.monotonic() > deadline:
                raise RuntimeError(f"query timed out while waiting for final result: {data}")
            time.sleep(1)
            data = self.request_json("GET", data["next_uri"])
        if data.get("state") != "Succeeded":
            raise RuntimeError(f"query did not finish in one response: {data}")
        return [tuple(row) for row in data.get("data") or []]

    def query_one(self, sql: str) -> tuple:
        rows = self.execute(sql)
        if len(rows) != 1:
            raise RuntimeError(f"expected one row, got {len(rows)} for SQL: {sql}")
        return rows[0]


def run_statements(db: Databend, statements: Iterable[str], dry_run: bool) -> None:
    for sql in statements:
        print(sql if dry_run else f"running: {sql.splitlines()[0][:100]}")
        if not dry_run:
            db.execute(sql)


def verify_session_settings(db: Databend, args: argparse.Namespace) -> None:
    value = db.query_one(
        "select value from system.settings where name = 'proxy_routing_model'"
    )[0]
    if value != args.proxy_routing_model:
        raise RuntimeError(
            "proxy_routing_model setting is not applied: "
            f"expected {args.proxy_routing_model!r}, got {value!r}"
        )

    if args.disable_distributed_pruning:
        value = db.query_one(
            "select value from system.settings where name = 'enable_distributed_pruning'"
        )[0]
        if value != "0":
            raise RuntimeError(
                "enable_distributed_pruning setting is not applied: "
                f"expected '0', got {value!r}"
            )


def setup_data(db: Databend, args: argparse.Namespace) -> None:
    trace_width = len(str(args.trace_cardinality - 1))
    chat_width = len(str(args.chat_cardinality - 1))
    user_width = len(str(args.user_cardinality - 1))
    if args.payload_bytes == 0:
        payload_expr = "''"
    else:
        payload_repeat = (args.payload_bytes + 31) // 32
        payload_expr = (
            f"substring(repeat(md5(to_string(number)), {payload_repeat}), "
            f"1, {args.payload_bytes})"
        )
    row_options = (
        f"row_per_block={args.row_per_block} block_per_segment={args.block_per_segment}"
    )

    statements = [
        f"drop table if exists {quote_ident(TABLE_PROXY)}",
        f"drop table if exists {quote_ident(TABLE_TRACE)}",
        f"drop table if exists {quote_ident(TABLE_CHAT)}",
        f"drop table if exists {quote_ident(TABLE_USER)}",
        f"drop table if exists {quote_ident(TABLE_TRACE_CHAT)}",
        f"drop table if exists {quote_ident(TABLE_TRACE_CHAT_USER)}",
        f"""
create table {quote_ident(TABLE_TRACE)}(
    start_time timestamp,
    trace_id string,
    chat_id string,
    user_id string,
    span_id string,
    route string,
    payload string
) cluster by(trace_id) {row_options}
""".strip(),
        f"""
create table {quote_ident(TABLE_CHAT)}(
    start_time timestamp,
    trace_id string,
    chat_id string,
    user_id string,
    span_id string,
    route string,
    payload string
) cluster by(chat_id) {row_options}
""".strip(),
        f"""
create table {quote_ident(TABLE_USER)}(
    start_time timestamp,
    trace_id string,
    chat_id string,
    user_id string,
    span_id string,
    route string,
    payload string
) cluster by(user_id) {row_options}
""".strip(),
        f"""
create table {quote_ident(TABLE_TRACE_CHAT)}(
    start_time timestamp,
    trace_id string,
    chat_id string,
    user_id string,
    span_id string,
    route string,
    payload string
) cluster by(trace_id, chat_id) {row_options}
""".strip(),
        f"""
create table {quote_ident(TABLE_TRACE_CHAT_USER)}(
    start_time timestamp,
    trace_id string,
    chat_id string,
    user_id string,
    span_id string,
    route string,
    payload string
) cluster by(trace_id, chat_id, user_id) {row_options}
""".strip(),
        f"""
insert into {quote_ident(TABLE_TRACE)}
select
    '2024-01-01 00:00:00'::timestamp,
    concat('trace_', lpad(to_string(number % {args.trace_cardinality}), {trace_width}, '0')),
    concat('chat_', lpad(to_string(number % {args.chat_cardinality}), {chat_width}, '0')),
    concat('user_', lpad(to_string(number % {args.user_cardinality}), {user_width}, '0')),
    concat('span_', to_string(number)),
    'trace',
    {payload_expr}
from numbers({args.rows})
""".strip(),
        f"""
insert into {quote_ident(TABLE_CHAT)}
select
    '2024-01-01 00:00:00'::timestamp,
    concat('trace_', lpad(to_string(number % {args.trace_cardinality}), {trace_width}, '0')),
    concat('chat_', lpad(to_string(number % {args.chat_cardinality}), {chat_width}, '0')),
    concat('user_', lpad(to_string(number % {args.user_cardinality}), {user_width}, '0')),
    concat('span_', to_string(number)),
    'chat',
    {payload_expr}
from numbers({args.rows})
""".strip(),
        f"""
insert into {quote_ident(TABLE_USER)}
select
    '2024-01-01 00:00:00'::timestamp,
    concat('trace_', lpad(to_string(number % {args.trace_cardinality}), {trace_width}, '0')),
    concat('chat_', lpad(to_string(number % {args.chat_cardinality}), {chat_width}, '0')),
    concat('user_', lpad(to_string(number % {args.user_cardinality}), {user_width}, '0')),
    concat('span_', to_string(number)),
    'user',
    {payload_expr}
from numbers({args.rows})
""".strip(),
        f"""
insert into {quote_ident(TABLE_TRACE_CHAT)}
select
    '2024-01-01 00:00:00'::timestamp,
    concat('trace_', lpad(to_string(number % {args.trace_cardinality}), {trace_width}, '0')),
    concat('chat_', lpad(to_string(number % {args.chat_cardinality}), {chat_width}, '0')),
    concat('user_', lpad(to_string(number % {args.user_cardinality}), {user_width}, '0')),
    concat('span_', to_string(number)),
    'trace_chat',
    {payload_expr}
from numbers({args.rows})
""".strip(),
        f"""
insert into {quote_ident(TABLE_TRACE_CHAT_USER)}
select
    '2024-01-01 00:00:00'::timestamp,
    concat('trace_', lpad(to_string(number % {args.trace_cardinality}), {trace_width}, '0')),
    concat('chat_', lpad(to_string(number % {args.chat_cardinality}), {chat_width}, '0')),
    concat('user_', lpad(to_string(number % {args.user_cardinality}), {user_width}, '0')),
    concat('span_', to_string(number)),
    'trace_chat_user',
    {payload_expr}
from numbers({args.rows})
""".strip(),
        f"alter table {quote_ident(TABLE_TRACE)} recluster final",
        f"alter table {quote_ident(TABLE_CHAT)} recluster final",
        f"alter table {quote_ident(TABLE_USER)} recluster final",
        f"alter table {quote_ident(TABLE_TRACE_CHAT)} recluster final",
        f"alter table {quote_ident(TABLE_TRACE_CHAT_USER)} recluster final",
        f"""
create table {quote_ident(TABLE_PROXY)}(
    start_time timestamp,
    trace_id string,
    chat_id string,
    user_id string,
    span_id string,
    route string,
    payload string
)
engine = proxy targets='{TABLE_TRACE},{TABLE_CHAT},{TABLE_USER},{TABLE_TRACE_CHAT},{TABLE_TRACE_CHAT_USER}' default='{TABLE_TRACE}'
""".strip(),
    ]

    run_statements(db, statements, args.dry_run)


def query_sql(table: str, predicate: str) -> str:
    return f"""
select min(route), count(), sum(length(payload))
from {quote_ident(table)}
where {predicate}
""".strip()


def explain_sql(table: str, predicate: str) -> str:
    return f"explain {query_sql(table, predicate)}"


def parse_int_value(value: str) -> int:
    return int(value.replace(",", "").strip())


def parse_size_bytes(value: str) -> int:
    match = re.fullmatch(
        r"\s*([0-9]+(?:\.[0-9]+)?)\s*([kmgtp]?i?b|b)\s*",
        value.replace(",", ""),
        flags=re.IGNORECASE,
    )
    if not match:
        raise ValueError(f"cannot parse read size: {value!r}")

    number = float(match.group(1))
    unit = match.group(2).lower()
    multipliers = {
        "b": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
        "pb": 1000**5,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
        "pib": 1024**5,
    }
    return int(number * multipliers[unit])


def parse_explain_stats(rows: list[tuple]) -> ScanStats:
    text = "\n".join(str(column) for row in rows for column in row)
    patterns = {
        "partitions_scanned": r"partitions scanned:\s*([0-9,]+)",
        "partitions_total": r"partitions total:\s*([0-9,]+)",
        "read_rows": r"read rows:\s*([0-9,]+)",
        "read_size": r"read size:\s*([0-9,.]+\s*[A-Za-z]+)",
    }
    values = {}
    for name, pattern in patterns.items():
        matches = re.findall(pattern, text, flags=re.IGNORECASE)
        if not matches:
            raise RuntimeError(f"EXPLAIN output does not contain {name}: {text}")
        values[name] = matches[-1]

    return ScanStats(
        partitions_scanned=parse_int_value(values["partitions_scanned"]),
        partitions_total=parse_int_value(values["partitions_total"]),
        read_rows=parse_int_value(values["read_rows"]),
        read_bytes=parse_size_bytes(values["read_size"]),
    )


def explain_stats(db: Databend, table: str, predicate: str) -> ScanStats:
    return parse_explain_stats(db.execute(explain_sql(table, predicate)))


def add_stats(left: ScanStats, right: ScanStats) -> ScanStats:
    return ScanStats(
        partitions_scanned=left.partitions_scanned + right.partitions_scanned,
        partitions_total=left.partitions_total + right.partitions_total,
        read_rows=left.read_rows + right.read_rows,
        read_bytes=left.read_bytes + right.read_bytes,
    )


def query_batch(db: Databend, table: str, predicates: tuple[str, ...]) -> QueryResult:
    start = time.perf_counter()
    rows = [db.query_one(query_sql(table, predicate)) for predicate in predicates]
    elapsed_ms = (time.perf_counter() - start) * 1000
    return QueryResult(rows=rows, elapsed_ms=elapsed_ms)


def measure_query_batches(
    db: Databend,
    tables: dict[str, str],
    predicates: tuple[str, ...],
    warmup_rounds: int,
    measure_rounds: int,
) -> dict[str, QueryResult]:
    names = list(tables.keys())
    rows_by_route: dict[str, list[tuple]] = {}
    timings_by_route = {name: [] for name in names}

    for _ in range(warmup_rounds):
        round_names = names.copy()
        random.shuffle(round_names)
        for name in round_names:
            query_batch(db, tables[name], predicates)

    for _ in range(measure_rounds):
        round_names = names.copy()
        random.shuffle(round_names)
        for name in round_names:
            result = query_batch(db, tables[name], predicates)
            rows_by_route[name] = result.rows
            timings_by_route[name].append(result.elapsed_ms)

    return {
        name: QueryResult(
            rows=rows_by_route[name],
            elapsed_ms=statistics.median(timings_by_route[name]),
        )
        for name in names
    }


def explain_batch(db: Databend, table: str, predicates: tuple[str, ...]) -> ScanStats:
    stats = [explain_stats(db, table, predicate) for predicate in predicates]
    total = stats[0]
    for item in stats[1:]:
        total = add_stats(total, item)
    return total


def run_case(
    db: Databend,
    case: BenchCase,
    args: argparse.Namespace,
) -> RouteResult:
    tables = {
        "proxy": TABLE_PROXY,
        "trace": TABLE_TRACE,
        "chat": TABLE_CHAT,
        "user": TABLE_USER,
        "trace_chat": TABLE_TRACE_CHAT,
        "trace_chat_user": TABLE_TRACE_CHAT_USER,
    }
    physical_routes = ["trace", "chat", "user", "trace_chat", "trace_chat_user"]
    stats_by_route = {
        name: explain_batch(db, table, case.predicates) for name, table in tables.items()
    }
    query_by_route = measure_query_batches(
        db,
        tables,
        case.predicates,
        args.warmup_rounds,
        args.measure_rounds,
    )
    rows = {name: result.rows for name, result in query_by_route.items()}
    query_ms_by_route = {
        name: result.elapsed_ms for name, result in query_by_route.items()
    }

    actual_routes = {row[0] for row in rows["proxy"] if row[0] is not None}
    actual_route = next(iter(actual_routes)) if len(actual_routes) == 1 else "mixed"
    actual_count = sum(int(row[1]) for row in rows["proxy"])
    best_cost = min(stats_by_route[name].cost for name in physical_routes)
    best_routes = [name for name in physical_routes if stats_by_route[name].cost == best_cost]
    best_route = "trace" if "trace" in best_routes else best_routes[0]
    route_ranks = rank_routes(stats_by_route, physical_routes)
    route_rank = route_ranks.get(actual_route)
    top1_match = actual_route in best_routes
    acceptable_route = route_rank is not None and route_rank <= MAX_ACCEPTABLE_RANK

    reasons = []
    if actual_route not in physical_routes:
        reasons.append(f"proxy route={actual_route!r} is not a physical target route")
    if actual_count != case.expected_count:
        reasons.append(f"proxy count={actual_count}, expected {case.expected_count}")
    for name in physical_routes:
        physical_count = sum(int(row[1]) for row in rows[name])
        if physical_count != case.expected_count:
            reasons.append(
                f"{name} count={physical_count}, expected {case.expected_count}"
            )
    if actual_route in physical_routes and stats_by_route["proxy"].cost != stats_by_route[
        actual_route
    ].cost:
        reasons.append(
            f"proxy explain cost={stats_by_route['proxy'].cost}, "
            f"selected target cost={stats_by_route[actual_route].cost}"
        )

    print_case_report(
        case=case,
        actual_route=actual_route,
        actual_count=actual_count,
        best_route=best_route,
        best_routes=best_routes,
        stats_by_route=stats_by_route,
        query_ms_by_route=query_ms_by_route,
        route_rank=route_rank,
        top1_match=top1_match,
        acceptable_route=acceptable_route,
        passed=not reasons,
        reason="; ".join(reasons),
    )

    return RouteResult(
        case_name=case.name,
        best_route=best_route,
        best_routes=best_routes,
        actual_route=actual_route,
        expected_count=case.expected_count,
        actual_count=actual_count,
        stats_by_route=stats_by_route,
        query_ms_by_route=query_ms_by_route,
        route_rank=route_rank,
        top1_match=top1_match,
        acceptable_route=acceptable_route,
        passed=not reasons,
        reason="; ".join(reasons),
    )


def format_bytes(value: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{value} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024


def format_cost(stats: ScanStats) -> str:
    return f"{stats.partitions_scanned}/{stats.read_rows}"


def format_cost_delta(stats: ScanStats, best: ScanStats) -> str:
    return (
        f"{stats.partitions_scanned - best.partitions_scanned:+d}/"
        f"{stats.read_rows - best.read_rows:+d}"
    )


def rank_routes(
    stats_by_route: dict[str, ScanStats],
    routes: list[str],
) -> dict[str, int]:
    return {
        name: rank
        for rank, name in enumerate(
            sorted(routes, key=lambda route: stats_by_route[route].cost),
            start=1,
        )
    }


def print_case_report(
    case: BenchCase,
    actual_route: str,
    actual_count: int,
    best_route: str,
    best_routes: list[str],
    stats_by_route: dict[str, ScanStats],
    query_ms_by_route: dict[str, float],
    route_rank: int | None,
    top1_match: bool,
    acceptable_route: bool,
    passed: bool,
    reason: str,
) -> None:
    print(f"\ncase: {case.name}")
    print(f"predicate: {'; '.join(case.predicates)}")
    print(
        f"best_by_stats={best_route} tied={','.join(best_routes)} "
        f"proxy_route={actual_route} count={actual_count}/{case.expected_count} "
        f"route_rank={route_rank or '-'} top1_match={top1_match} "
        f"acceptable_route={acceptable_route} passed={passed}"
    )
    best_stats = stats_by_route[best_route]
    ranks = rank_routes(
        stats_by_route,
        ["trace", "chat", "user", "trace_chat", "trace_chat_user"],
    )
    print(
        f"{'route':<18} {'pick':<18} {'rank':>4} {'query_ms(p50)':>13} "
        f"{'partitions':>14} {'read_rows':>12} {'read_size':>12} "
        f"{'route_cost(parts/rows)':<24} {'delta_best(parts/rows)'}"
    )
    print("-" * 143)
    for name in ["proxy", "trace", "chat", "user", "trace_chat", "trace_chat_user"]:
        stats = stats_by_route[name]
        markers = []
        if name == "proxy":
            markers.append(f"->{actual_route}")
        if name == best_route:
            markers.append("best")
        if name == actual_route:
            markers.append("chosen")
        print(
            f"{name:<18} {','.join(markers):<18} "
            f"{ranks.get(name, '-'):>4} "
            f"{query_ms_by_route[name]:>13.2f} "
            f"{stats.partitions_scanned:>6}/{stats.partitions_total:<7} "
            f"{stats.read_rows:>12} {format_bytes(stats.read_bytes):>12} "
            f"{format_cost(stats):<24} {format_cost_delta(stats, best_stats)}"
        )
    if reason:
        print(f"reason: {reason}")


def print_summary(results: list[RouteResult]) -> None:
    print("\nsummary:")
    print(
        "case,best_by_stats,tied_best,actual_route,count,"
        "best_route_cost,proxy_route_cost,selected_route_cost,selected_delta,"
        "best_ms,proxy_ms,selected_ms,route_rank,top1_match,acceptable_route,passed"
    )
    for result in results:
        selected_cost = (
            format_cost(result.stats_by_route[result.actual_route])
            if result.actual_route in result.stats_by_route
            else None
        )
        selected_delta = (
            format_cost_delta(
                result.stats_by_route[result.actual_route],
                result.stats_by_route[result.best_route],
            )
            if result.actual_route in result.stats_by_route
            else None
        )
        selected_ms = (
            f"{result.query_ms_by_route[result.actual_route]:.2f}"
            if result.actual_route in result.query_ms_by_route
            else None
        )
        print(
            f"{result.case_name},{result.best_route},{'|'.join(result.best_routes)},"
            f"{result.actual_route},{result.actual_count},"
            f"{format_cost(result.stats_by_route[result.best_route])},"
            f"{format_cost(result.stats_by_route['proxy'])},"
            f"{selected_cost},{selected_delta},"
            f"{result.query_ms_by_route[result.best_route]:.2f},"
            f"{result.query_ms_by_route['proxy']:.2f},"
            f"{selected_ms},{result.route_rank},{result.top1_match},"
            f"{result.acceptable_route},{result.passed}"
        )
        if not result.passed:
            print(f"  reason: {result.reason}")


def print_route_quality(
    results: list[RouteResult],
    required_top1_hit_ratio: float,
    required_acceptable_hit_ratio: float,
) -> tuple[float, float]:
    total = len(results)
    top1_hits = sum(result.top1_match for result in results)
    acceptable_hits = sum(result.acceptable_route for result in results)
    top1_hit_ratio = top1_hits / total if total else 0.0
    acceptable_hit_ratio = acceptable_hits / total if total else 0.0

    print("\nroute quality:")
    print(
        f"top1_hit_ratio={top1_hits}/{total}={top1_hit_ratio:.2%}, "
        f"required_top1_hit_ratio={required_top1_hit_ratio:.2%}, "
        f"acceptable_hit_ratio={acceptable_hits}/{total}={acceptable_hit_ratio:.2%}, "
        f"required_acceptable_hit_ratio={required_acceptable_hit_ratio:.2%}"
    )
    return top1_hit_ratio, acceptable_hit_ratio


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Diagnose Databend PROXY table routing with EXPLAIN scan statistics."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="default")
    parser.add_argument("--connect-timeout", type=int, default=600)
    parser.add_argument("--rows", type=int, default=2_000_000)
    parser.add_argument("--trace-cardinality", type=int, default=17)
    parser.add_argument("--chat-cardinality", type=int, default=19)
    parser.add_argument("--user-cardinality", type=int, default=59)
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--row-per-block", type=int, default=200)
    parser.add_argument("--block-per-segment", type=int, default=10)
    parser.add_argument(
        "--warmup-rounds",
        type=int,
        default=2,
        help="Query rounds per route/case to run before latency measurement.",
    )
    parser.add_argument(
        "--measure-rounds",
        type=int,
        default=5,
        help="Query rounds per route/case used to report median latency.",
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Reuse existing proxy benchmark tables.",
    )
    parser.add_argument(
        "--keep-distributed-pruning",
        dest="disable_distributed_pruning",
        action="store_false",
        help="Leave enable_distributed_pruning unchanged for the session.",
    )
    parser.set_defaults(disable_distributed_pruning=True)
    parser.add_argument(
        "--enable-proxy-bloom-pruning",
        action="store_true",
        help="Enable bloom index pruning during PROXY lightweight route estimation.",
    )
    parser.add_argument(
        "--proxy-routing-model",
        choices=("statistics", "prefix"),
        default="statistics",
        help="PROXY routing model to benchmark.",
    )
    parser.add_argument(
        "--min-top1-hit-ratio",
        type=float,
        default=None,
        help="Override the minimum top-1 route hit ratio required by this run.",
    )
    parser.add_argument(
        "--min-acceptable-hit-ratio",
        type=float,
        default=None,
        help="Override the minimum acceptable route hit ratio required by this run.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.rows < 101:
        raise ValueError("--rows must be at least 101")
    if args.trace_cardinality < 11:
        raise ValueError("--trace-cardinality must be at least 11")
    if args.chat_cardinality < 11:
        raise ValueError("--chat-cardinality must be at least 11")
    if args.user_cardinality < 11:
        raise ValueError("--user-cardinality must be at least 11")
    if args.payload_bytes < 0:
        raise ValueError("--payload-bytes cannot be negative")
    if args.row_per_block < 1:
        raise ValueError("--row-per-block must be positive")
    if args.block_per_segment < 1:
        raise ValueError("--block-per-segment must be positive")
    if args.warmup_rounds < 0:
        raise ValueError("--warmup-rounds cannot be negative")
    if args.measure_rounds < 1:
        raise ValueError("--measure-rounds must be positive")
    if args.min_top1_hit_ratio is not None and not 0 <= args.min_top1_hit_ratio <= 1:
        raise ValueError("--min-top1-hit-ratio must be between 0 and 1")
    if (
        args.min_acceptable_hit_ratio is not None
        and not 0 <= args.min_acceptable_hit_ratio <= 1
    ):
        raise ValueError("--min-acceptable-hit-ratio must be between 0 and 1")


def min_top1_hit_ratio(args: argparse.Namespace) -> float:
    if args.min_top1_hit_ratio is not None:
        return args.min_top1_hit_ratio
    if args.proxy_routing_model == "prefix":
        return 0.0
    return MIN_TOP1_HIT_RATIO


def min_acceptable_hit_ratio(args: argparse.Namespace) -> float:
    if args.min_acceptable_hit_ratio is not None:
        return args.min_acceptable_hit_ratio
    if args.proxy_routing_model == "prefix":
        return PREFIX_MIN_ACCEPTABLE_HIT_RATIO
    return STATISTICS_MIN_ACCEPTABLE_HIT_RATIO


def main() -> int:
    args = parse_args()
    validate_args(args)

    trace_width = len(str(args.trace_cardinality - 1))
    chat_width = len(str(args.chat_cardinality - 1))
    user_width = len(str(args.user_cardinality - 1))
    cases = build_cases(
        args.rows,
        args.trace_cardinality,
        trace_width,
        args.chat_cardinality,
        chat_width,
        args.user_cardinality,
        user_width,
    )

    db = Databend(args)
    try:
        if args.dry_run:
            if not args.skip_setup:
                setup_data(db, args)
            return 0

        verify_session_settings(db, args)
        if not args.skip_setup:
            setup_data(db, args)

        results = [run_case(db, case, args) for case in cases]
        print_summary(results)
        required_top1_hit_ratio = min_top1_hit_ratio(args)
        required_acceptable_hit_ratio = min_acceptable_hit_ratio(args)
        top1_hit_ratio, acceptable_hit_ratio = print_route_quality(
            results,
            required_top1_hit_ratio,
            required_acceptable_hit_ratio,
        )

        failed = [result for result in results if not result.passed]
        if failed:
            print(f"\nfailed cases: {len(failed)}", file=sys.stderr)
            return 1
        if top1_hit_ratio < required_top1_hit_ratio:
            print(
                f"\ntop1 hit ratio {top1_hit_ratio:.2%} is below "
                f"required {required_top1_hit_ratio:.2%}",
                file=sys.stderr,
            )
            return 1
        if acceptable_hit_ratio < required_acceptable_hit_ratio:
            print(
                f"\nacceptable hit ratio {acceptable_hit_ratio:.2%} is below "
                f"required {required_acceptable_hit_ratio:.2%}",
                file=sys.stderr,
            )
            return 1

        print("\nall proxy routing benchmark cases passed")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
