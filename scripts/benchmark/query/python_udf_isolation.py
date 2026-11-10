#!/usr/bin/env python3
# Copyright 2026 Datafuse Labs
# Licensed under the Apache License, Version 2.0.
"""Measure identical Python scalar UDF SQL against two separately built servers.

Uses only the Python standard library. Start each server with the same config,
run with --label main / isolated, then compare the JSON reports. --pid enables
Linux /proc sampling (RSS is a summed upper bound, not unique physical memory).
"""

import argparse
import base64
import concurrent.futures
import json
import math
import os
import platform
import statistics
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path


class Client:
    def __init__(self, endpoint, user, password, threads):
        self.endpoint = endpoint.rstrip("/")
        credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
        }
        self.settings = {
            "max_threads": str(threads),
            "enable_query_result_cache": "0",
        }
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(self, uri, body=None):
        url = urllib.parse.urljoin(self.endpoint + "/", uri)
        if (
            urllib.parse.urlsplit(url).netloc
            != urllib.parse.urlsplit(self.endpoint).netloc
        ):
            raise RuntimeError("Refusing a query continuation on another server")
        request = urllib.request.Request(
            url,
            data=None if body is None else json.dumps(body).encode(),
            headers=self.headers,
        )
        with self.opener.open(request, timeout=600) as response:
            return json.load(response)

    def query(self, sql):
        response = self.request(
            "/v1/query",
            {
                "sql": sql,
                "session": {"settings": self.settings},
                "pagination": {"wait_time_secs": 1},
            },
        )
        data = []
        while True:
            if response.get("error"):
                raise RuntimeError(response["error"])
            data.extend(response.get("data", []))
            next_uri = response.get("next_uri")
            if not next_uri:
                break
            response = self.request(next_uri)
        return data


def process_sample(root):
    """Count root and all descendant processes; sample RSS from statm."""
    pids = [root]
    seen = {root}
    for pid in pids:
        for path in Path(f"/proc/{pid}/task").glob("*/children"):
            try:
                children = [int(value) for value in path.read_text().split()]
            except (OSError, ValueError):
                continue
            for child in children:
                if child not in seen:
                    seen.add(child)
                    pids.append(child)
    rss = 0
    for pid in pids:
        try:
            rss += int(Path(f"/proc/{pid}/statm").read_text().split()[1]) * os.sysconf(
                "SC_PAGE_SIZE"
            )
        except (OSError, ValueError, IndexError):
            continue
    return {"processes": len(pids), "rss_bytes_sum": rss}


class Sampler:
    def __init__(self, pid):
        self.pid = pid
        self.stop = threading.Event()
        self.peak = {"processes": 0, "rss_bytes_sum": 0}
        self.thread = None

    def start(self):
        if self.pid:
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()

    def run(self):
        while not self.stop.is_set():
            sample = process_sample(self.pid)
            for key, value in sample.items():
                self.peak[key] = max(self.peak[key], value)
            self.stop.wait(0.02)

    def finish(self):
        self.stop.set()
        if self.thread:
            self.thread.join()
        return self.peak if self.pid else None


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)]


def benchmark(args):
    def client():
        return Client(args.endpoint, args.user, args.password, args.threads)

    connection = client()
    version = connection.query("SELECT version()")
    definitions = [
        ("bench_py_add", "INT64", "def handler(x):\n    return x + 1"),
        (
            "bench_py_cpu",
            "INT64",
            (
                "def handler(x):\n"
                "    for _ in range(32):\n"
                "        x = (x * 1664525 + 1013904223) & 2147483647\n"
                "    return x"
            ),
        ),
    ]
    if args.workload in ("gcd", "gcd-scaling"):
        definitions = [
            (
                "bench_py_gcd",
                "INT64, INT64",
                (
                    "def handler(a, b):\n"
                    "    a, b = abs(a), abs(b)\n"
                    "    while b:\n"
                    "        a, b = b, a % b\n"
                    "    return a"
                ),
            ),
            (
                "bench_py_math_gcd",
                "INT64, INT64",
                "import math\ndef handler(a, b):\n    return math.gcd(a, b)",
            ),
        ]
    for name, signature, code in definitions:
        connection.query(
            f"CREATE OR REPLACE FUNCTION {name}({signature}) RETURNS INT64 "
            f"LANGUAGE python HANDLER = 'handler' AS $$\n{code}\n$$"
        )

    report = {
        "label": args.label,
        "revision": args.revision,
        "version": version,
        "platform": platform.platform(),
        "threads": args.threads,
        "repetitions": args.repetitions,
        "workload": args.workload,
        "udf_definitions": {name: code for name, _, code in definitions},
        "note": "SQL end-to-end wall time; cold_first precedes all Python execution; "
        "subsequent queries are warm-server but isolated workers are fresh per SQL. "
        "RSS sum double-counts shared pages; 20ms process sampling can miss short workers.",
        "results": [],
    }
    workloads = [
        ("cold_first", "bench_py_add", 1, 1, 1),
        ("scalar", "bench_py_add", 1, 1, args.repetitions),
        ("small_batch", "bench_py_add", 1000, 1, args.repetitions),
        ("large_batch", "bench_py_add", 1000000, 1, args.repetitions),
        ("cpu", "bench_py_cpu", 100000, 1, args.repetitions),
        ("concurrent_scalar", "bench_py_add", 1, 8, args.repetitions),
        ("concurrent_batch", "bench_py_add", 100000, 8, args.repetitions),
    ]
    expected_gcd = {}
    if args.workload in ("gcd", "gcd-scaling"):
        workloads = [
            ("gcd_cold_first", "bench_py_gcd", 1, 1, 1),
            ("gcd_scalar", "bench_py_gcd", 1, 1, args.repetitions),
            ("gcd_small_batch", "bench_py_gcd", 1000, 1, args.repetitions),
            ("gcd_large_batch", "bench_py_gcd", 1000000, 1, args.repetitions),
            ("gcd_concurrent_batch", "bench_py_gcd", 100000, 8, args.repetitions),
            (
                "gcd_concurrent_large_batch",
                "bench_py_gcd",
                1000000,
                8,
                args.repetitions,
            ),
            ("math_gcd_large_batch", "bench_py_math_gcd", 1000000, 1, args.repetitions),
            (
                "math_gcd_concurrent_batch",
                "bench_py_math_gcd",
                100000,
                8,
                args.repetitions,
            ),
        ]
        if args.workload == "gcd-scaling":
            workloads = [
                ("gcd_cold_first", "bench_py_gcd", 1, 1, 1),
                ("gcd_scalar", "bench_py_gcd", 1, 1, args.repetitions),
                ("gcd_scaling", "bench_py_gcd", args.gcd_rows, 1, args.repetitions),
            ]
        # Independent oracle, outside the measured SQL region. Both inputs vary
        # per row; this is not a constant expression or an artificial busy loop.
        for rows in {workload[2] for workload in workloads}:
            expected_gcd[rows] = sum(
                math.gcd(1000000007 + 104729 * n, 1000000009 + 130363 * n)
                for n in range(rows)
            )
        report["gcd_inputs"] = [
            "1000000007 + 104729 * number",
            "1000000009 + 130363 * number",
        ]
        report["expected_sums_math_gcd"] = expected_gcd
    try:
        for name, function, rows, concurrency, repetitions in workloads:
            arguments = "to_int64(number)"
            expected = rows * (rows + 1) // 2 if function == "bench_py_add" else None
            if args.workload in ("gcd", "gcd-scaling"):
                arguments = (
                    "to_int64(1000000007 + 104729 * number), "
                    "to_int64(1000000009 + 130363 * number)"
                )
                expected = expected_gcd[rows]
            sql = f"SELECT sum({function}({arguments})) FROM numbers({rows})"

            def run_one(sql=sql, expected=expected):
                started = time.perf_counter()
                result = client().query(sql)
                elapsed = time.perf_counter() - started
                if expected is not None and int(result[0][0]) != expected:
                    raise ValueError(
                        f"Incorrect UDF result: {result!r}; expected {expected}"
                    )
                return elapsed, result

            sampler = Sampler(args.pid)
            sampler.start()
            elapsed = []
            outputs = []
            walls = []
            try:
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=concurrency
                ) as pool:
                    for _ in range(repetitions):
                        started = time.perf_counter()
                        samples = list(
                            pool.map(lambda _: run_one(), range(concurrency))
                        )
                        walls.append(time.perf_counter() - started)
                        elapsed.extend(sample[0] for sample in samples)
                        outputs.extend(sample[1] for sample in samples)
            finally:
                peak = sampler.finish()
            result = {
                "name": name,
                "sql": sql,
                "rows_per_query": rows,
                "concurrency": concurrency,
                "samples_secs": elapsed,
                "round_wall_secs": walls,
                "median_secs": statistics.median(elapsed),
                "p95_secs": percentile(elapsed, 0.95),
                "rows_per_sec": rows * concurrency * repetitions / sum(walls),
                "peak_process_sample": peak,
                "output": outputs[0],
            }
            report["results"].append(result)
            Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(result), flush=True)
    finally:
        for name, _, _ in definitions:
            connection.query(f"DROP FUNCTION IF EXISTS {name}")


def compare(paths):
    base, changed = (json.loads(Path(path).read_text()) for path in paths)
    results = {result["name"]: result for result in changed["results"]}
    print(
        "| Workload | main median ms | isolated median ms | latency change | throughput change |"
    )
    print("|---|---:|---:|---:|---:|")
    for before in base["results"]:
        after = results[before["name"]]
        for key in ("rows_per_query", "concurrency", "output"):
            if before[key] != after[key]:
                raise ValueError(f"Mismatched workload {before['name']}: {key}")
        latency = after["median_secs"] / before["median_secs"] - 1
        throughput = after["rows_per_sec"] / before["rows_per_sec"] - 1
        print(
            f"| {before['name']} | {1000 * before['median_secs']:.2f} | "
            f"{1000 * after['median_secs']:.2f} | {latency:+.1%} | {throughput:+.1%} |"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compare", nargs=2, metavar=("MAIN_JSON", "ISOLATED_JSON"))
    parser.add_argument("--endpoint", default="http://127.0.0.1:8000")
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument(
        "--workload", choices=("basic", "gcd", "gcd-scaling"), default="basic"
    )
    parser.add_argument(
        "--gcd-rows",
        type=int,
        default=5000000,
        help="Rows in the single-query gcd-scaling workload",
    )
    parser.add_argument("--pid", type=int)
    parser.add_argument("--label", default="isolated")
    parser.add_argument("--revision", default="unknown")
    parser.add_argument("--output", default="python-udf-benchmark.json")
    args = parser.parse_args()
    if args.compare:
        compare(args.compare)
    else:
        if args.repetitions < 1 or args.threads < 1 or args.gcd_rows < 1:
            parser.error("--repetitions, --threads and --gcd-rows must be positive")
        benchmark(args)


if __name__ == "__main__":
    main()
