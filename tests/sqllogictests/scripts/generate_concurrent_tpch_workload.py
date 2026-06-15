#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import random
import shutil
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TEMPLATE = REPO_ROOT / "tests/sqllogictests/suites/tpch/concurrent/templates.sql"
DEFAULT_OUTPUT = REPO_ROOT / "tests/sqllogictests/suites/tpch/concurrent/generated"

NATIONS = [
    "ALGERIA",
    "ARGENTINA",
    "BRAZIL",
    "CANADA",
    "EGYPT",
    "FRANCE",
    "GERMANY",
    "INDIA",
    "JAPAN",
    "ROMANIA",
    "RUSSIA",
    "SAUDI ARABIA",
    "UNITED KINGDOM",
    "UNITED STATES",
]

PART_TOKENS = ["green", "almond", "antique", "blue", "burnished", "chartreuse", "forest"]
START_DATES = ["1992-01-01", "1993-04-01", "1994-07-01", "1995-10-01", "1996-01-01"]


def read_templates(path: Path) -> dict[str, str]:
    templates = {}
    current_name = None
    current_lines = []

    for line in path.read_text().splitlines():
        if line.startswith("-- template:"):
            current_name = line.split(":", 1)[1].strip()
            current_lines = []
        elif line == "-- endtemplate":
            if current_name is None:
                raise ValueError("template end marker without a template name")
            templates[current_name] = "\n".join(current_lines).strip() + "\n"
            current_name = None
            current_lines = []
        elif current_name is not None:
            current_lines.append(line)

    if current_name is not None:
        raise ValueError(f"template {current_name!r} is missing an end marker")
    return templates


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_values(rows: list[tuple[int, int, str, str]]) -> str:
    values = []
    for row_id, flag, amount, payload in rows:
        values.append(f"({row_id}, {flag}, {amount}, {sql_string(payload)})")
    return ", ".join(values)


def write_case(output_dir: Path, prefix: str, index: int, content: str) -> None:
    path = output_dir / f"{prefix}_{index:03d}.test"
    path.write_text(content)


def heavy_context(rng: random.Random, index: int) -> dict[str, object]:
    return {
        "case_id": index,
        "max_threads": rng.choice([2, 3, 4]),
        "nation": rng.choice(NATIONS),
        "limit": rng.randint(40, 120),
    }


def explain_context(rng: random.Random, index: int) -> dict[str, object]:
    return {
        "case_id": index,
        "profit_divisor": rng.choice([5, 10, 20]),
        "truncate_scale": rng.choice([10, 50, 100]),
        "part_token": rng.choice(PART_TOKENS),
        "limit": rng.randint(3, 12),
    }


def merge_context(rng: random.Random, index: int) -> dict[str, object]:
    target_start = rng.randint(0, 16)
    target_size = rng.randint(18, 34)
    source_start = target_start + rng.randint(-8, 10)
    source_size = rng.randint(18, 36)

    target_ids = list(range(target_start, target_start + target_size))
    source_ids = list(range(source_start, source_start + source_size))
    if set(source_ids).issubset(set(target_ids)):
        source_ids.extend(range(max(target_ids) + 1, max(target_ids) + 1 + rng.randint(3, 8)))

    target_rows = [
        (
            row_id,
            rng.randint(0, 9),
            f"{rng.randint(1000, 9000)}.{rng.randint(0, 99):02d}",
            f"target_{index}_{row_id}",
        )
        for row_id in target_ids
    ]
    source_rows = [
        (
            row_id,
            rng.randint(0, 9),
            f"{rng.randint(1000, 9000)}.{rng.randint(0, 99):02d}",
            f"source_{index}_{row_id}",
        )
        for row_id in source_ids
    ]

    target_id_set = set(target_ids)
    source_mod = rng.randint(3, 7)
    source_skip = rng.randrange(source_mod)
    flag_threshold = rng.randint(3, 7)
    insert_min_id = rng.choice(source_ids)

    matched_count = sum(
        1
        for row_id, flag, _, _ in source_rows
        if row_id in target_id_set and row_id % source_mod != source_skip and flag >= flag_threshold
    )
    if matched_count == 0:
        source_mod = max(source_mod, 2)
        source_skip = source_ids[0] % source_mod
        flag_threshold = 0
        matched_count = sum(
            1
            for row_id, _, _, _ in source_rows
            if row_id in target_id_set and row_id % source_mod != source_skip
        )
    if matched_count == 0:
        source_mod = 97
        source_skip = 96
        flag_threshold = 0
        matched_count = sum(1 for row_id, _, _, _ in source_rows if row_id in target_id_set)

    insert_count = sum(
        1 for row_id, _, _, _ in source_rows if row_id >= insert_min_id and row_id not in target_id_set
    )
    if insert_count == 0:
        insert_min_id = min(row_id for row_id in source_ids if row_id not in target_id_set)
        insert_count = sum(
            1 for row_id, _, _, _ in source_rows if row_id >= insert_min_id and row_id not in target_id_set
        )

    return {
        "case_id": index,
        "database": f"concurrent_merge_{index:03d}",
        "target_values": sql_values(target_rows),
        "source_values": sql_values(source_rows),
        "source_mod": source_mod,
        "source_skip": source_skip,
        "flag_threshold": flag_threshold,
        "insert_min_id": insert_min_id,
        "matched_count": matched_count,
        "insert_count": insert_count,
    }


def insert_replace_context(rng: random.Random, index: int) -> dict[str, object]:
    high_amount = rng.randint(80000, 160000)
    return {
        "case_id": index,
        "database": f"concurrent_insert_{index:03d}",
        "id_offset": index * 1_000_000,
        "start_date": rng.choice(START_DATES),
        "months": rng.randint(1, 5),
        "acctbal_threshold": rng.randint(-500, 5000),
        "insert_limit": rng.randint(40, 180),
        "high_amount": high_amount,
        "replace_min_amount": max(0, high_amount - rng.randint(5000, 25000)),
        "overwrite_min_amount": max(0, high_amount - rng.randint(20000, 50000)),
        "tag_suffix": rng.choice(["bulk", "ci", "merge", "planner", "stress"]),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate randomized sqllogictest files for the TPC-H concurrent CI workload."
    )
    parser.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--workers", type=int, default=int(os.getenv("CONCURRENT_TPCH_WORKERS", "64")))
    parser.add_argument("--rounds", type=int, default=int(os.getenv("CONCURRENT_TPCH_ROUNDS", "4")))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = args.seed
    if seed is None:
        env_seed = os.getenv("CONCURRENT_TPCH_SEED")
        seed = int(env_seed) if env_seed else random.SystemRandom().randrange(1, 2**31)

    rng = random.Random(seed)
    templates = read_templates(args.template)
    if args.workers <= 0:
        raise ValueError("--workers must be greater than 0")
    if args.rounds <= 0:
        raise ValueError("--rounds must be greater than 0")

    shutil.rmtree(args.output, ignore_errors=True)
    args.output.mkdir(parents=True, exist_ok=True)

    header = (
        f"# Generated by tests/sqllogictests/scripts/generate_concurrent_tpch_workload.py\n"
        f"# seed: {seed}\n"
        f"# generated_at_unix: {int(time.time())}\n\n"
        f"statement ok\n"
        f"set flight_client_timeout = 10;\n\n"
    )

    for worker_index in range(args.workers):
        parts = [header]
        for round_index in range(args.rounds):
            case_index = worker_index * args.rounds + round_index
            parts.append(f"# worker: {worker_index}, round: {round_index}\n\n")
            parts.append(templates["heavy_tpch_q21"].format(**heavy_context(rng, case_index)))
            parts.append("\n")
            parts.append(templates["explain_tpch_q9"].format(**explain_context(rng, case_index)))
            parts.append("\n")
            parts.append(templates["merge_into"].format(**merge_context(rng, case_index)))
            parts.append("\n")
            parts.append(templates["insert_replace"].format(**insert_replace_context(rng, case_index)))
            parts.append("\n")
        write_case(args.output, "00_worker", worker_index, "".join(parts))

    total_cases = args.workers * args.rounds
    print(
        f"Generated {args.workers} concurrent TPC-H sqllogictest files "
        f"({args.rounds} rounds, {total_cases} randomized case bundles) in {args.output}"
    )
    print(f"CONCURRENT_TPCH_SEED={seed}")


if __name__ == "__main__":
    main()
