#!/usr/bin/env python3
"""Three-node databend-meta cluster benchmark.

Starts one leader and two followers, imports the same exported seed data into
all three raft dirs, drives load through node1 with databend-metabench, and
prints the metabench summary.

The seed file is used as-is. Prepare or sanitize it before invoking this script.

Example:

    scripts/benchmark/meta-cluster-bench.py \
      --node1-bin ~/bin/v1.2.819-nightly \
      --node2-bin ~/bin/v1.2.819-nightly \
      --node3-bin ~/bin/v1.2.908-nightly \
      --seed-file ~/data/backup.data \
      --node1-label 819 --node2-label 819 --node3-label 908
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

NODE_TOML = """\
admin_api_address       = "0.0.0.0:{admin_port}"
grpc_api_address        = "0.0.0.0:{grpc_port}"
grpc_api_advertise_host = "127.0.0.1"

[log]
[log.stderr]
on = false
[log.file]
on = true
level = "INFO"
format = "json"
dir = "{log_dir}"

[raft_config]
id            = {id}
raft_dir      = "{raft_dir}"
raft_api_port = {raft_port}
raft_listen_host    = "127.0.0.1"
raft_advertise_host = "localhost"
snapshot_logs_since_last = {snapshot_logs_since_last}
install_snapshot_timeout = 60000
max_applied_log_to_keep = 10240
snapshot_db_block_keys = 8000
snapshot_db_block_cache_size = 1073741824
{mode}
"""


@dataclass
class Node:
    node_id: int
    label: str
    bin_dir: Path
    admin_port: int
    grpc_port: int
    raft_port: int

    @property
    def meta_bin(self) -> Path:
        return self.bin_dir / "databend-meta"


@dataclass
class RunResult:
    workload_ops: int
    elapsed_ms: int | None = None
    qps: float | None = None
    success_qps: float | None = None
    error_qps: float | None = None
    client_pool_size: int | None = None
    metabench_total: int | None = None
    metabench_success: int | None = None
    metabench_error: int | None = None
    latency_avg_us: int | None = None
    latency_max_us: int | None = None
    latency_p50_us: int | None = None
    latency_p90_us: int | None = None
    latency_p95_us: int | None = None
    latency_p99_us: int | None = None
    latency_histogram: str | None = None
    notes: list[str] = field(default_factory=list)


def main() -> int:
    args = parse_args()
    nodes = build_nodes(args)
    bench_bin = args.node1_bin / "databend-metabench"
    metactl_bin = args.metactl_bin or args.node1_bin / "databend-metactl"

    check_inputs(nodes, bench_bin, metactl_bin, args.seed_file)
    reset_work_dir(args.work_dir)
    import_seed_data(metactl_bin, nodes, args.seed_file, args.work_dir)
    write_configs(nodes, args.work_dir, args.snapshot_logs_since_last)

    result = run_cluster(nodes, bench_bin, metactl_bin, args)
    print_report(result, nodes, args)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one three-node databend-meta benchmark.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--node1-bin", type=Path, required=True)
    parser.add_argument("--node2-bin", type=Path, required=True)
    parser.add_argument("--node3-bin", type=Path, required=True)
    parser.add_argument("--node1-label")
    parser.add_argument("--node2-label")
    parser.add_argument("--node3-label")
    parser.add_argument(
        "--seed-file",
        type=Path,
        required=True,
        help="exported meta data file; used as-is by databend-metactl import.",
    )
    parser.add_argument(
        "--metactl-bin",
        type=Path,
        help="databend-metactl used for import and transfer-leader "
             "(default: --node1-bin/databend-metactl).",
    )
    parser.add_argument("--workload-clients", type=int, default=4)
    parser.add_argument("--workload-client-pool-size", type=int, default=1)
    parser.add_argument("--workload-ops", type=int, default=10000)
    parser.add_argument("--workload-rpc", default="upsert_kv")
    parser.add_argument("--port-base", type=int, default=28101)
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path("/tmp/databend-meta-cluster-bench"),
        help="scratch dir for configs, logs, and raft state; wiped on each run.",
    )
    parser.add_argument(
        "--snapshot-logs-since-last",
        type=int,
        default=1_000_000_000,
        help="large default avoids snapshot generation during benchmark.",
    )
    parser.add_argument("--post-bench-wait-secs", type=float, default=2.0)
    args = parser.parse_args()
    resolve_paths(args)
    return args


def resolve_paths(args: argparse.Namespace) -> None:
    for attr in ("node1_bin", "node2_bin", "node3_bin", "seed_file", "work_dir"):
        setattr(args, attr, getattr(args, attr).expanduser().resolve())
    if args.metactl_bin is not None:
        args.metactl_bin = args.metactl_bin.expanduser().resolve()


def build_nodes(args: argparse.Namespace) -> list[Node]:
    base = args.port_base
    return [
        Node(1, args.node1_label or args.node1_bin.name, args.node1_bin,
             base, base + 1, base + 2),
        Node(2, args.node2_label or args.node2_bin.name, args.node2_bin,
             base + 100, base + 101, base + 102),
        Node(3, args.node3_label or args.node3_bin.name, args.node3_bin,
             base + 200, base + 201, base + 202),
    ]


def check_inputs(
    nodes: list[Node],
    bench_bin: Path,
    metactl_bin: Path,
    seed_file: Path,
) -> None:
    required = [(f"node{node.node_id} databend-meta", node.meta_bin) for node in nodes]
    required.extend([
        ("node1 databend-metabench", bench_bin),
        ("databend-metactl", metactl_bin),
        ("seed file", seed_file),
    ])

    missing = [f"{label}: {path}" for label, path in required if not path.exists()]
    if missing:
        raise FileNotFoundError("missing required paths:\n  " + "\n  ".join(missing))


def reset_work_dir(work_dir: Path) -> None:
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True)
    for node_id in (1, 2, 3):
        (work_dir / f"logs{node_id}").mkdir()


def import_seed_data(
    metactl_bin: Path,
    nodes: list[Node],
    seed_file: Path,
    work_dir: Path,
) -> None:
    initial_cluster = [
        f"{node.node_id}=localhost:{node.raft_port}" for node in nodes
    ]
    for node in nodes:
        raft_dir = work_dir / f"raft{node.node_id}"
        cmd = [
            str(metactl_bin),
            "import",
            "--raft-dir",
            str(raft_dir),
            "--id",
            str(node.node_id),
            "--db",
            str(seed_file),
        ]
        for item in initial_cluster:
            cmd.extend(["--initial-cluster", item])

        print(f"[seed] import node{node.node_id} raft_dir={raft_dir}", flush=True)
        subprocess.run(cmd, check=True)


def write_configs(
    nodes: list[Node],
    work_dir: Path,
    snapshot_logs_since_last: int,
) -> None:
    leader_raft_port = nodes[0].raft_port
    for node in nodes:
        if node.node_id == 1:
            mode = "single = true"
        else:
            mode = f'join = ["127.0.0.1:{leader_raft_port}"]'

        cfg = NODE_TOML.format(
            id=node.node_id,
            admin_port=node.admin_port,
            grpc_port=node.grpc_port,
            raft_port=node.raft_port,
            raft_dir=work_dir / f"raft{node.node_id}",
            log_dir=work_dir / f"logs{node.node_id}",
            snapshot_logs_since_last=snapshot_logs_since_last,
            mode=mode,
        )
        (work_dir / f"node{node.node_id}.toml").write_text(cfg)


def run_cluster(
    nodes: list[Node],
    bench_bin: Path,
    metactl_bin: Path,
    args: argparse.Namespace,
) -> RunResult:
    result = RunResult(
        workload_ops=args.workload_clients * args.workload_ops,
    )
    procs: list[tuple[str, subprocess.Popen]] = []

    try:
        print_setup(nodes, bench_bin, args)
        for node in nodes:
            print(f"[start] node{node.node_id} {node.label}", flush=True)
            proc = spawn_meta(
                node.meta_bin,
                args.work_dir / f"node{node.node_id}.toml",
                args.work_dir / f"node{node.node_id}.stdout.log",
                args.work_dir,
            )
            procs.append((f"node{node.node_id}", proc))

        for node in nodes:
            wait_for_health(node.admin_port, f"node{node.node_id}", timeout=60)

        status = transfer_leader_to_node1(nodes, metactl_bin)
        print(f"[cluster] leader={extract_leader(status)} voters={sorted(extract_voters(status))}")

        run_metabench(nodes[0], bench_bin, args)
        parse_metabench(args.work_dir / "metabench.log", result)

        if args.post_bench_wait_secs > 0:
            time.sleep(args.post_bench_wait_secs)

        print(f"[summary] qps={fmt_float(result.qps)} elapsed_ms={fmt_int(result.elapsed_ms)}")
    except Exception as e:
        result.notes.append(f"error: {e}")
        print(f"[ERROR] {e}", file=sys.stderr)
    finally:
        stop_processes(procs)

    return result


def print_setup(nodes: list[Node], bench_bin: Path, args: argparse.Namespace) -> None:
    for node in nodes:
        print(
            f"[setup] node{node.node_id} {node.label} -> {node.meta_bin} "
            f"ports admin/grpc/raft={node.admin_port}/{node.grpc_port}/{node.raft_port}",
            flush=True,
        )
    print(f"[setup] seed    = {args.seed_file}", flush=True)
    print(f"[setup] bench   = {bench_bin}", flush=True)
    print(f"[setup] workdir = {args.work_dir}", flush=True)
    print("", flush=True)


def run_metabench(node: Node, bench_bin: Path, args: argparse.Namespace) -> None:
    bench_log = args.work_dir / "metabench.log"
    print(
        f"[bench] grpc=127.0.0.1:{node.grpc_port} "
        f"client={args.workload_clients} "
        f"number={args.workload_ops} "
        f"client_pool_size={args.workload_client_pool_size} "
        f"rpc={args.workload_rpc}",
        flush=True,
    )

    with bench_log.open("w") as out:
        subprocess.run(
            [
                str(bench_bin),
                "--grpc-api-address",
                f"127.0.0.1:{node.grpc_port}",
                "--client",
                str(args.workload_clients),
                "--client-pool-size",
                str(args.workload_client_pool_size),
                "--number",
                str(args.workload_ops),
                "--rpc",
                args.workload_rpc,
                "--log-level",
                "warn",
            ],
            cwd=args.work_dir,
            stdout=out,
            stderr=subprocess.STDOUT,
            check=True,
        )
    print(f"[bench] log={bench_log}", flush=True)


def parse_metabench(log_path: Path, result: RunResult) -> None:
    text = log_path.read_text(errors="replace")
    summary_line = next(
        (line for line in text.splitlines() if line.startswith("benchmark summary:")),
        None,
    )
    if summary_line:
        metrics = parse_key_value_line(summary_line, "benchmark summary:")
        result.metabench_total = parse_int_metric(metrics, "total")
        result.metabench_success = parse_int_metric(metrics, "success")
        result.metabench_error = parse_int_metric(metrics, "error")
        result.elapsed_ms = parse_int_metric(metrics, "elapsed_ms")
        result.qps = parse_float_metric(metrics, "qps")
        result.success_qps = parse_float_metric(metrics, "success_qps")
        result.error_qps = parse_float_metric(metrics, "error_qps")
        result.client_pool_size = parse_int_metric(metrics, "client_pool_size")
        result.latency_avg_us = parse_int_metric(metrics, "avg_us")
        result.latency_max_us = parse_int_metric(metrics, "max_us")
        result.latency_p50_us = parse_int_metric(metrics, "p50_us")
        result.latency_p90_us = parse_int_metric(metrics, "p90_us")
        result.latency_p95_us = parse_int_metric(metrics, "p95_us")
        result.latency_p99_us = parse_int_metric(metrics, "p99_us")
    else:
        fallback = next(
            (line for line in text.splitlines() if line.startswith("benchmark client(")),
            None,
        )
        result.elapsed_ms = parse_metabench_ms(fallback) if fallback else None
        if result.elapsed_ms:
            result.qps = result.workload_ops * 1000.0 / result.elapsed_ms
        else:
            result.notes.append("metabench produced no summary line")

    histogram_line = next(
        (line for line in text.splitlines()
         if line.startswith("benchmark latency histogram:")),
        None,
    )
    if histogram_line:
        result.latency_histogram = histogram_line.split(":", 1)[1].strip()


def print_report(
    result: RunResult,
    nodes: list[Node],
    args: argparse.Namespace,
) -> None:
    print(f"\n{'=' * 72}\n=== Report\n{'=' * 72}\n")
    for node in nodes:
        print(
            f"node{node.node_id}: {node.label} "
            f"(grpc=127.0.0.1:{node.grpc_port}, raft=localhost:{node.raft_port})"
        )
    client_pool_size = result.client_pool_size or args.workload_client_pool_size
    print(
        f"workload: {args.workload_clients} clients x {args.workload_ops} "
        f"ops/client = {result.workload_ops} ops via {args.workload_rpc}; "
        f"client_pool_size={client_pool_size}"
    )
    print(f"seed    : {args.seed_file}")
    print(f"work_dir: {args.work_dir}\n")

    rows = [
        ("throughput (ops/sec)", fmt_float(result.qps)),
        ("success qps", fmt_float(result.success_qps)),
        ("error qps", fmt_float(result.error_qps)),
        ("wall (ms)", fmt_int(result.elapsed_ms)),
        ("metabench total", fmt_int(result.metabench_total)),
        ("metabench success", fmt_int(result.metabench_success)),
        ("metabench error", fmt_int(result.metabench_error)),
        ("latency avg/max (us)", fmt_pair(result.latency_avg_us, result.latency_max_us)),
        ("latency p50/p90 (us)", fmt_pair(result.latency_p50_us, result.latency_p90_us)),
        ("latency p95/p99 (us)", fmt_pair(result.latency_p95_us, result.latency_p99_us)),
    ]
    label_w = max(len(label) for label, _ in rows)
    for label, value in rows:
        print(f"  {label.ljust(label_w)}  {value}")

    if result.latency_histogram:
        print("\nlatency histogram:")
        print(f"  {result.latency_histogram}")

    if result.notes:
        print("\nnotes:")
        for note in result.notes:
            print(f"  - {note}")


def spawn_meta(binary: Path, cfg: Path, stdout_path: Path, cwd: Path) -> subprocess.Popen:
    stdout = open(stdout_path, "w")
    proc = subprocess.Popen(
        [str(binary), "-c", str(cfg)],
        cwd=cwd,
        stdout=stdout,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    stdout.close()
    return proc


def stop_processes(procs: list[tuple[str, subprocess.Popen]]) -> None:
    for name, proc in reversed(procs):
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait()
        print(f"[teardown] {name}: exited (rc={proc.returncode})", flush=True)


def transfer_leader_to_node1(nodes: list[Node], metactl_bin: Path) -> dict:
    status = wait_cluster(nodes, timeout=90)
    if extract_leader(status) == 1:
        return status

    leader = extract_leader(status)
    leader_node = next((node for node in nodes if node.node_id == leader), nodes[0])
    subprocess.run(
        [
            str(metactl_bin),
            "transfer-leader",
            "--to",
            "1",
            "--admin-api-address",
            f"127.0.0.1:{leader_node.admin_port}",
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    deadline = time.time() + 30
    last: object = None
    while time.time() < deadline:
        try:
            status = cluster_status(nodes[0].admin_port)
            if extract_leader(status) == 1:
                return status
        except Exception as e:
            last = e
        time.sleep(0.5)
    if last is not None:
        print(f"[cluster] transfer-leader polling did not confirm node1: {last}")
    return wait_cluster(nodes, timeout=30)


def wait_cluster(nodes: list[Node], timeout: float) -> dict:
    deadline = time.time() + timeout
    last: object = None
    while time.time() < deadline:
        for node in nodes:
            try:
                status = cluster_status(node.admin_port)
                last = status
                if len(extract_voters(status)) >= 3 and extract_leader(status) is not None:
                    return status
            except Exception as e:
                last = e
        time.sleep(0.5)
    raise RuntimeError(f"timeout waiting for 3-node cluster; last: {last}")


def wait_for_health(admin_port: int, name: str, timeout: float) -> None:
    url = f"http://127.0.0.1:{admin_port}/v1/health"
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with _http_get(url, timeout=1) as response:
                if 200 <= response.status < 300:
                    return
        except (urllib.error.URLError, ConnectionError, socket.timeout, OSError) as e:
            last_err = e
        time.sleep(0.3)
    raise RuntimeError(f"{name}: timeout waiting for {url} (last err: {last_err})")


def cluster_status(admin_port: int) -> dict:
    with _http_get(f"http://127.0.0.1:{admin_port}/v1/cluster/status", timeout=1) as r:
        return json.loads(r.read())


def _http_get(url: str, timeout: float):
    return _NO_PROXY_OPENER.open(url, timeout=timeout)


def extract_voters(status: dict) -> set[int | str]:
    voters = status.get("voters")
    if isinstance(voters, list):
        return {node_id_value(item) for item in voters}

    metrics = status.get("metrics", {})
    membership = {}
    if isinstance(metrics, dict):
        membership = metrics.get("membership_config", {}).get("membership", {})

    ids = set()
    configs = membership.get("configs", []) if isinstance(membership, dict) else []
    for cfg in configs:
        if isinstance(cfg, list):
            ids.update(node_id_value(item) for item in cfg)
    return ids


def extract_leader(status: dict) -> int | str | None:
    for key in ("leader", "current_leader"):
        if status.get(key) is not None:
            return node_id_value(status[key])

    metrics = status.get("metrics", {})
    if isinstance(metrics, dict):
        leader = metrics.get("current_leader") or metrics.get("leader")
        if leader is not None:
            return node_id_value(leader)
    return None


def node_id_value(value) -> int | str:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value) if value.isdigit() else value
    if isinstance(value, dict):
        for key in ("node_id", "id", "name", "leader_id"):
            if key in value:
                return node_id_value(value[key])
    return json.dumps(value, sort_keys=True)


def parse_key_value_line(line: str, prefix: str) -> dict[str, str]:
    if not line.startswith(prefix):
        return {}
    metrics = {}
    for part in line[len(prefix):].strip().split():
        if "=" in part:
            key, value = part.split("=", 1)
            metrics[key] = value.rstrip(",")
    return metrics


def parse_int_metric(metrics: dict[str, str], key: str) -> int | None:
    try:
        return int(metrics[key])
    except (KeyError, ValueError):
        return None


def parse_float_metric(metrics: dict[str, str], key: str) -> float | None:
    try:
        return float(metrics[key])
    except (KeyError, ValueError):
        return None


def parse_metabench_ms(line: str | None) -> int | None:
    if line is None:
        return None
    parts = line.split(" in ")
    if len(parts) != 2:
        return None
    try:
        return int(parts[1].split(" ")[0])
    except ValueError:
        return None


def fmt_int(value: int | None) -> str:
    return "n/a" if value is None else f"{value}"


def fmt_float(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}"


def fmt_pair(a: int | None, b: int | None) -> str:
    if a is None or b is None:
        return "n/a"
    return f"{a}/{b}"


if __name__ == "__main__":
    sys.exit(main())
