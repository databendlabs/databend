#!/usr/bin/env python3
"""
Generate a monomorphization report for databend-common-functions.

Run from src/query/functions/tests/it:

    python3 mono_stats_report.py

By default this runs:

    cargo rustc -p databend-common-functions --lib --profile test -- \
        -Z dump-mono-stats=y -Z dump-mono-stats-format=json -Z print-mono-items=y

Outputs are written below ./mono-stats-report. The wrapper graph is derived
from rustc MONO_ITEM lines by extracting concrete adaptors_v2 type paths; it
does not inspect object files or maintain a hard-coded list of combinator edges.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
from collections import Counter
from pathlib import Path
from typing import Iterable


MONO_ITEM_RE = re.compile(r"^MONO_ITEM\s+(\S+)\s+(.+?)\s+@@\s+")
ADAPTOR_TYPE_RE = re.compile(
    r"aggregate_function_v2_impl::adaptors_v2::"
    r"([A-Za-z_][A-Za-z0-9_]*)::([A-Z][A-Za-z0-9_]*)"
)
AGGR_IMPL_RE = re.compile(
    r" as databend_common_expression::aggregate::aggregate_function_v2::AggrImpl>"
    r"::([A-Za-z_][A-Za-z0-9_]*)"
)
AGGREGATE_FUNCTION_RE = re.compile(
    r"aggregate_function_v2_impl::([A-Za-z_][A-Za-z0-9_]*)::"
)


def find_workspace_root(start: Path) -> Path:
    for path in [start, *start.parents]:
        manifest = path / "Cargo.toml"
        if manifest.exists() and "[workspace]" in manifest.read_text(errors="ignore"):
            return path
    raise SystemExit(f"could not find workspace Cargo.toml from {start}")


def write_csv(path: Path, header: list[str], rows: Iterable[tuple]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def run_cargo(args: argparse.Namespace, root: Path, out_dir: Path) -> Path:
    stdout_path = out_dir / "cargo-rustc-mono.stdout.log"
    command_path = out_dir / "cargo-rustc-mono.command.txt"

    cmd = [
        "cargo",
        "rustc",
        "--manifest-path",
        str(root / "Cargo.toml"),
        "-p",
        args.package,
        "--lib",
        "--profile",
        args.profile,
        "--",
        "-Z",
        "dump-mono-stats=.",
        "-Z",
        "dump-mono-stats-format=json",
        "-Z",
        "print-mono-items=y",
    ]
    env = os.environ.copy()
    if args.target_dir:
        env["CARGO_TARGET_DIR"] = str(Path(args.target_dir).resolve())

    command = " ".join(cmd)
    if args.target_dir:
        command += f"\nCARGO_TARGET_DIR={env['CARGO_TARGET_DIR']}"
    command_path.write_text(command + "\n")

    with stdout_path.open("w") as stdout:
        subprocess.run(cmd, cwd=out_dir, env=env, stdout=stdout, check=True)

    return stdout_path


def iter_mono_items(paths: Iterable[Path]):
    for path in paths:
        if not path.exists():
            continue
        with path.open(errors="ignore") as f:
            for line in f:
                match = MONO_ITEM_RE.match(line.rstrip())
                if match:
                    yield match.group(1), match.group(2)


def adaptor_stack(symbol: str) -> tuple[str, ...]:
    stack: list[str] = []
    for match in ADAPTOR_TYPE_RE.finditer(symbol):
        label = f"{match.group(1)}::{match.group(2)}"
        if not stack or stack[-1] != label:
            stack.append(label)
    return tuple(stack)


def module_bucket(symbol: str) -> str:
    match = ADAPTOR_TYPE_RE.search(symbol)
    if match:
        return match.group(1)
    if "aggregate_function_v2_impl::adaptors_v2::" in symbol:
        return "adaptors_v2"
    match = AGGREGATE_FUNCTION_RE.search(symbol)
    if match:
        return f"function::{match.group(1)}"
    return "unknown"


def method_bucket(symbol: str) -> str:
    match = AGGR_IMPL_RE.search(symbol)
    if match:
        return "AggrImpl::" + match.group(1)
    if "create_ordered_aggregate_function" in symbol:
        return "create_ordered_aggregate_function"
    if "create_named_aggregate_function" in symbol:
        return "create_named_aggregate_function"
    if "create_aggregate_function" in symbol:
        return "create_aggregate_function"
    if "drop_in_place" in symbol or "drop" in symbol:
        return "std/glue::drop"
    if "alloc::vec" in symbol or "Vec<" in symbol:
        return "std/glue::vec"
    if "core::iter" in symbol or "Iterator" in symbol:
        return "std/glue::iter"
    if "core::sync" in symbol or "alloc::sync" in symbol or "Arc<" in symbol:
        return "std/glue::sync"
    if "core::ptr" in symbol:
        return "std/glue::ptr"
    return "other"


def read_mono_stats_json(paths: Iterable[Path]) -> Counter:
    stats = Counter()
    for path in paths:
        try:
            items = json.loads(path.read_text(errors="ignore"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            count = item.get("instantiation_count")
            if isinstance(name, str) and isinstance(count, int):
                stats[name] += count
    return stats


def analyze(logs: list[Path], mono_json: list[Path]):
    item_kinds = Counter()
    module_counts = Counter()
    method_counts = Counter()
    node_counts = Counter()
    edge_counts = Counter()
    stack_counts = Counter()
    function_counts = Counter()
    total_items = 0
    adaptor_items = 0

    for kind, symbol in iter_mono_items(logs):
        total_items += 1
        item_kinds[kind] += 1
        if "aggregate_function_v2_impl::" not in symbol:
            continue

        function_match = AGGREGATE_FUNCTION_RE.search(symbol)
        if function_match:
            function_counts[function_match.group(1)] += 1

        if "aggregate_function_v2_impl::adaptors_v2" not in symbol:
            continue

        adaptor_items += 1
        method_counts[method_bucket(symbol)] += 1

        stack = adaptor_stack(symbol)
        if not stack:
            module_counts[module_bucket(symbol)] += 1
            continue
        stack_counts[stack] += 1
        for node in stack:
            node_counts[node] += 1
            module_counts[node.split("::", 1)[0]] += 1
        for outer, inner in zip(stack, stack[1:]):
            edge_counts[(outer, inner)] += 1

    return {
        "total_items": total_items,
        "adaptor_items": adaptor_items,
        "item_kinds": item_kinds,
        "module_counts": module_counts,
        "method_counts": method_counts,
        "node_counts": node_counts,
        "edge_counts": edge_counts,
        "stack_counts": stack_counts,
        "function_counts": function_counts,
        "mono_stats": read_mono_stats_json(mono_json),
    }


def write_report(out_dir: Path, data: dict, logs: list[Path], mono_json: list[Path]) -> Path:
    write_csv(
        out_dir / "wrapper_edges.csv",
        ["outer", "inner", "mono_items"],
        ((a, b, count) for (a, b), count in data["edge_counts"].most_common()),
    )
    write_csv(
        out_dir / "wrapper_nodes.csv",
        ["node", "mono_items"],
        data["node_counts"].most_common(),
    )
    write_csv(
        out_dir / "wrapper_stacks.csv",
        ["stack", "mono_items"],
        ((" -> ".join(stack), count) for stack, count in data["stack_counts"].most_common()),
    )
    write_csv(
        out_dir / "modules.csv",
        ["module", "mono_items"],
        data["module_counts"].most_common(),
    )
    write_csv(
        out_dir / "methods.csv",
        ["bucket", "mono_items"],
        data["method_counts"].most_common(),
    )
    write_csv(
        out_dir / "functions.csv",
        ["function_or_module", "mono_items"],
        data["function_counts"].most_common(),
    )
    write_csv(
        out_dir / "item_kinds.csv",
        ["kind", "mono_items"],
        data["item_kinds"].most_common(),
    )
    write_csv(
        out_dir / "mono_stats.csv",
        ["name", "instantiation_count"],
        data["mono_stats"].most_common(),
    )

    dot_path = out_dir / "wrapper_edges.dot"
    with dot_path.open("w") as f:
        f.write("digraph mono_wrapper_edges {\n")
        f.write("  rankdir=LR;\n")
        f.write("  node [shape=box, fontsize=10];\n")
        for node, count in data["node_counts"].most_common():
            f.write(f'  "{node}" [label="{node}\\n{count} items"];\n')
        for (outer, inner), count in data["edge_counts"].most_common(120):
            f.write(f'  "{outer}" -> "{inner}" [label="{count}"];\n')
        f.write("}\n")

    report_path = out_dir / "mono-stats-report.md"
    lines = [
        "# Mono Stats Report",
        "",
        f"- total mono items: {data['total_items']}",
        f"- aggregate v2 adaptor mono items: {data['adaptor_items']}",
        "- rustc input:",
        *[f"  - `{path}`" for path in logs],
        *[f"  - `{path}`" for path in mono_json],
        "",
        "## Wrapper Edges",
        "",
        "| outer | inner | mono items |",
        "|---|---|---:|",
    ]
    for (outer, inner), count in data["edge_counts"].most_common(30):
        lines.append(f"| `{outer}` | `{inner}` | {count} |")

    lines.extend(["", "## Wrapper Nodes", "", "| node | mono items |", "|---|---:|"])
    for node, count in data["node_counts"].most_common(30):
        lines.append(f"| `{node}` | {count} |")

    lines.extend(["", "## Adaptor Modules", "", "| module | mono items |", "|---|---:|"])
    for module, count in data["module_counts"].most_common(30):
        lines.append(f"| `{module}` | {count} |")

    lines.extend(["", "## Methods And Glue", "", "| bucket | mono items |", "|---|---:|"])
    for bucket, count in data["method_counts"].most_common(30):
        lines.append(f"| `{bucket}` | {count} |")

    lines.extend(["", "## Aggregate V2 Functions", "", "| function/module | mono items |", "|---|---:|"])
    for function, count in data["function_counts"].most_common(30):
        lines.append(f"| `{function}` | {count} |")

    lines.extend(["", "## Mono Stats Json", "", "| name | instantiations |", "|---|---:|"])
    for name, count in data["mono_stats"].most_common(30):
        lines.append(f"| `{name}` | {count} |")

    lines.extend(["", "## Wrapper Stacks", "", "| stack | mono items |", "|---|---:|"])
    for stack, count in data["stack_counts"].most_common(30):
        lines.append(f"| `{' -> '.join(stack)}` | {count} |")

    lines.extend(
        [
            "",
            "## Generated Files",
            "",
            f"- `{out_dir / 'wrapper_edges.csv'}`",
            f"- `{out_dir / 'wrapper_nodes.csv'}`",
            f"- `{out_dir / 'wrapper_stacks.csv'}`",
            f"- `{out_dir / 'modules.csv'}`",
            f"- `{out_dir / 'methods.csv'}`",
            f"- `{out_dir / 'functions.csv'}`",
            f"- `{out_dir / 'item_kinds.csv'}`",
            f"- `{out_dir / 'mono_stats.csv'}`",
            f"- `{dot_path}`",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n")
    return report_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", default="databend-common-functions")
    parser.add_argument("--profile", default="test")
    parser.add_argument("--target-dir")
    parser.add_argument("--out-dir", default="mono-stats-report")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument(
        "--log",
        action="append",
        type=Path,
        help="Use an existing rustc stdout/stderr log. Can be passed multiple times.",
    )
    parser.add_argument(
        "--mono-json",
        action="append",
        type=Path,
        help="Use an existing rustc dump-mono-stats json file. Can be passed multiple times.",
    )
    args = parser.parse_args()

    start = Path.cwd().resolve()
    root = find_workspace_root(start)
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = start / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    before_json = set(out_dir.rglob("*.mono_items.json"))
    logs: list[Path] = []
    if not args.no_build:
        stdout_path = run_cargo(args, root, out_dir)
        logs.extend([stdout_path])
    if args.log:
        logs.extend(args.log)
    if not logs:
        raise SystemExit("no input logs; either omit --no-build or pass --log")

    mono_json = list(args.mono_json or [])
    mono_json.extend(sorted(set(out_dir.rglob("*.mono_items.json")) - before_json))
    if not mono_json:
        mono_json.extend(sorted(out_dir.rglob("*.mono_items.json")))

    data = analyze(logs, mono_json)
    report_path = write_report(out_dir, data, logs, mono_json)
    print(report_path)
    print(out_dir / "wrapper_edges.csv")
    print(out_dir / "wrapper_edges.dot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
