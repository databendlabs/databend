#!/usr/bin/env python3
"""Two-node databend-meta cluster benchmark.

Brings up a 2-node databend-meta cluster (one leader + one follower),
drives load through the leader's gRPC endpoint with `databend-metabench`,
and prints a report (throughput, wall time, slow-IO counts per node).

The leader and follower can be different builds. Same build for both
gives a standard performance run; two different builds measure the cost
of a feature or regression at a rolling-upgrade boundary.

Usage
-----
    meta-cluster-bench.py \\
        --leader-bin   /path/to/build-A \\
        --follower-bin /path/to/build-B \\
        [--leader-label A] [--follower-label B] \\
        [--workload-clients 4] [--workload-ops 10000] [--workload-rpc upsert_kv] \\
        [--port-base 28101] [--work-dir DIR]

Each `--*-bin` directory is expected to contain:
  - databend-meta       (server, required for both leader and follower)
  - databend-metabench  (load client, required only in --leader-bin)

`databend-metabench` runs from the leader's directory because all
workload RPCs go to the leader; the client/server protocol is pinned
by the leader's build.


Example:

    scripts/benchmark/meta-cluster-bench.py \
      --leader-bin   ~/databend-1.2.896/target/release \
      --follower-bin ~/databend-1.2.896/target/release \
      --leader-label   v260428.3.0 \
      --follower-label v260428.3.0 \
      --port-base 30101


Output
------
- per-node configs, raft state, server logs under <work-dir>/
- workload log:                                  <work-dir>/metabench.log
- final report:                                  stdout
"""

import argparse
import json
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

# All admin-API requests target 127.0.0.1; build an opener that ignores
# environment proxy settings so a host-wide HTTP_PROXY (e.g. Clash) does
# not intercept loopback traffic and return 404/502.
_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _http_get(url: str, timeout: float):
    return _NO_PROXY_OPENER.open(url, timeout=timeout)


LEADER_TOML = """\
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
single        = true
snapshot_logs_since_last = 10000000
"""

FOLLOWER_TOML = """\
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
join          = ["127.0.0.1:{leader_raft_port}"]
snapshot_logs_since_last = 10000000
"""


@dataclass
class RunResult:
    leader_label: str
    follower_label: str
    workload_ops: int
    elapsed_ms: int | None = None
    qps: float | None = None
    slow_rpc_client: int = 0
    slow_io_leader: int = 0
    slow_io_follower: int = 0
    done_slowly_leader: int = 0
    done_slowly_follower: int = 0
    notes: list[str] = field(default_factory=list)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="2-node databend-meta cluster benchmark with optional "
                    "different builds for leader and follower.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--leader-bin", type=Path, required=True,
        help="directory with databend-meta and databend-metabench used by "
             "the leader (and the workload client).",
    )
    parser.add_argument(
        "--follower-bin", type=Path, required=True,
        help="directory with databend-meta used by the follower. "
             "Use the same path as --leader-bin for a same-build run.",
    )
    parser.add_argument(
        "--leader-label", default=None,
        help="display label for the leader build (default: --leader-bin dir name).",
    )
    parser.add_argument(
        "--follower-label", default=None,
        help="display label for the follower build (default: --follower-bin dir name).",
    )
    parser.add_argument(
        "--workload-clients", type=int, default=4,
        help="metabench --client (default: 4).",
    )
    parser.add_argument(
        "--workload-ops", type=int, default=10000,
        help="metabench --number = ops per client (default: 10000).",
    )
    parser.add_argument(
        "--workload-rpc", default="upsert_kv",
        help="metabench --rpc operation type (default: upsert_kv).",
    )
    parser.add_argument(
        "--port-base", type=int, default=28101,
        help="first port for the cluster; node 1 uses port-base..+2 "
             "(admin/grpc/raft), node 2 uses port-base+100..+102 (default: 28101).",
    )
    parser.add_argument(
        "--work-dir", type=Path,
        default=Path("/tmp/databend-meta-cluster-bench"),
        help="scratch dir for configs, logs, raft state. Wiped on each run "
             "(default: /tmp/databend-meta-cluster-bench).",
    )
    args = parser.parse_args()

    leader_meta   = args.leader_bin / "databend-meta"
    leader_bench  = args.leader_bin / "databend-metabench"
    follower_meta = args.follower_bin / "databend-meta"

    for label, p in (
        ("--leader-bin/databend-meta", leader_meta),
        ("--leader-bin/databend-metabench", leader_bench),
        ("--follower-bin/databend-meta", follower_meta),
    ):
        if not p.exists():
            sys.exit(f"missing {label}: {p}")

    leader_label = args.leader_label or args.leader_bin.name
    follower_label = args.follower_label or args.follower_bin.name

    if args.work_dir.exists():
        shutil.rmtree(args.work_dir)
    args.work_dir.mkdir(parents=True)

    ports = dict(
        n1_admin=args.port_base + 0,
        n1_grpc =args.port_base + 1,
        n1_raft =args.port_base + 2,
        n2_admin=args.port_base + 100,
        n2_grpc =args.port_base + 101,
        n2_raft =args.port_base + 102,
    )

    result = run_cluster(
        leader_meta=leader_meta,
        follower_meta=follower_meta,
        bench_bin=leader_bench,
        leader_label=leader_label,
        follower_label=follower_label,
        ports=ports,
        work_dir=args.work_dir,
        args=args,
    )

    print_report(result, args, ports)
    return 0


def run_cluster(
    *,
    leader_meta: Path,
    follower_meta: Path,
    bench_bin: Path,
    leader_label: str,
    follower_label: str,
    ports: dict[str, int],
    work_dir: Path,
    args: argparse.Namespace,
) -> RunResult:
    raft1 = work_dir / "raft1"
    raft2 = work_dir / "raft2"
    log1 = work_dir / "logs1"
    log2 = work_dir / "logs2"
    for d in (raft1, raft2, log1, log2):
        d.mkdir(parents=True, exist_ok=True)

    cfg1 = work_dir / "node1.toml"
    cfg2 = work_dir / "node2.toml"
    cfg1.write_text(LEADER_TOML.format(
        id=1,
        admin_port=ports["n1_admin"],
        grpc_port=ports["n1_grpc"],
        raft_port=ports["n1_raft"],
        raft_dir=raft1,
        log_dir=log1,
    ))
    cfg2.write_text(FOLLOWER_TOML.format(
        id=2,
        admin_port=ports["n2_admin"],
        grpc_port=ports["n2_grpc"],
        raft_port=ports["n2_raft"],
        raft_dir=raft2,
        log_dir=log2,
        leader_raft_port=ports["n1_raft"],
    ))

    print(f"[setup] leader   = {leader_label} -> {leader_meta}")
    print(f"[setup] follower = {follower_label} -> {follower_meta}")
    print(f"[setup] bench    = {bench_bin}")
    print(f"[setup] node1 ports admin/grpc/raft = "
          f"{ports['n1_admin']}/{ports['n1_grpc']}/{ports['n1_raft']}")
    print(f"[setup] node2 ports admin/grpc/raft = "
          f"{ports['n2_admin']}/{ports['n2_grpc']}/{ports['n2_raft']}")
    print(f"[setup] work_dir = {work_dir}\n")

    result = RunResult(
        leader_label=leader_label,
        follower_label=follower_label,
        workload_ops=args.workload_clients * args.workload_ops,
    )

    procs: list[tuple[str, subprocess.Popen]] = []
    try:
        print(f"[start] leader (id=1, {leader_label}) ...")
        p1 = spawn_meta(leader_meta, cfg1, work_dir / "node1.stdout.log", work_dir)
        procs.append(("leader", p1))
        wait_for_health(ports["n1_admin"], "node1", timeout=30)
        wait_for_voters(ports["n1_admin"], expected=1, name="node1", timeout=15)
        print("[start] leader is up and self-elected\n")

        print(f"[start] follower (id=2, {follower_label}) ...")
        p2 = spawn_meta(follower_meta, cfg2, work_dir / "node2.stdout.log", work_dir)
        procs.append(("follower", p2))
        wait_for_health(ports["n2_admin"], "node2", timeout=30)
        wait_for_voters(ports["n1_admin"], expected=2, name="node1", timeout=30)
        print("[start] cluster has 2 voters\n")

        bench_log = work_dir / "metabench.log"
        print(f"[bench] driving load through node1 grpc=127.0.0.1:{ports['n1_grpc']}")
        print(f"        client={args.workload_clients} number={args.workload_ops} "
              f"rpc={args.workload_rpc}")
        t0 = time.time()
        with open(bench_log, "w") as f:
            subprocess.run(
                [
                    str(bench_bin),
                    "--grpc-api-address", f"127.0.0.1:{ports['n1_grpc']}",
                    "--client", str(args.workload_clients),
                    "--number", str(args.workload_ops),
                    "--rpc", args.workload_rpc,
                    "--log-level", "warn",
                ],
                cwd=work_dir,
                stdout=f,
                stderr=subprocess.STDOUT,
            )
        wall = time.time() - t0
        print(f"[bench] wall={wall:.1f}s, log={bench_log}\n")

        bench_text = bench_log.read_text()
        summary_line = next(
            (ln for ln in bench_text.splitlines() if ln.startswith("benchmark client(")),
            None,
        )
        if summary_line:
            ms = parse_metabench_ms(summary_line)
            if ms is not None:
                result.elapsed_ms = ms
                result.qps = result.workload_ops * 1000.0 / ms
        else:
            result.notes.append("metabench produced no summary line")

        result.slow_rpc_client = bench_text.count("done slowly:")
        result.slow_io_leader = sum_count(log1, "Slow IO operation")
        result.slow_io_follower = sum_count(log2, "Slow IO operation")
        result.done_slowly_leader = sum_count(log1, "done slowly")
        result.done_slowly_follower = sum_count(log2, "done slowly")

        if result.qps is not None:
            print(f"[summary] qps={result.qps:.1f} ops/s, "
                  f"slow_io leader/follower="
                  f"{result.slow_io_leader}/{result.slow_io_follower}")
    except Exception as e:
        result.notes.append(f"error: {e}")
        print(f"[ERROR] {e}", file=sys.stderr)
    finally:
        for name, p in procs:
            if p.poll() is not None:
                continue
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
            print(f"[teardown] {name}: exited (rc={p.returncode})")

    return result


def print_report(
    result: RunResult, args: argparse.Namespace, ports: dict[str, int]
) -> None:
    print(f"\n{'=' * 72}\n=== Report\n{'=' * 72}\n")
    print(f"leader   : {result.leader_label}  (node1 grpc=127.0.0.1:{ports['n1_grpc']})")
    print(f"follower : {result.follower_label}  (node2 grpc=127.0.0.1:{ports['n2_grpc']})")
    print(f"workload : {args.workload_clients} clients x {args.workload_ops} "
          f"ops/client = {result.workload_ops} ops via {args.workload_rpc}\n")

    rows = [
        ("throughput (ops/sec)",  fmt_float(result.qps)),
        ("wall (ms)",             fmt_int(result.elapsed_ms)),
        ("slow-RPC (client view)", fmt_int(result.slow_rpc_client)),
        ("slow-IO leader",        fmt_int(result.slow_io_leader)),
        ("slow-IO follower",      fmt_int(result.slow_io_follower)),
        ("done-slowly leader",    fmt_int(result.done_slowly_leader)),
        ("done-slowly follower",  fmt_int(result.done_slowly_follower)),
    ]
    label_w = max(len(label) for label, _ in rows)
    for label, value in rows:
        print(f"  {label.ljust(label_w)}  {value}")

    if result.notes:
        print("\nnotes:")
        for n in result.notes:
            print(f"  - {n}")

    print(
        "\nLegend:\n"
        "  throughput (ops/sec)    successful RPCs per second, derived from\n"
        "                          metabench's summary line.\n"
        "  wall (ms)               total wall-clock duration of the workload,\n"
        "                          as reported by metabench.\n"
        "  slow-RPC (client view)  metabench-side count of RPCs that took\n"
        "                          longer than 300 ms end-to-end (`done slowly:`\n"
        "                          warnings from the meta gRPC client).\n"
        "  slow-IO leader/follower server-side count of disk-IO callbacks (raft-\n"
        "                          log flush/append) that took longer than 50 ms\n"
        "                          (`Slow IO operation` warnings). Primary signal\n"
        "                          for fsync / write-stall regressions.\n"
        "  done-slowly leader/follower\n"
        "                          server-side count of outbound gRPC calls\n"
        "                          (meta-to-meta forwarding etc.) that took\n"
        "                          longer than 300 ms. Typically 0 in a 2-node\n"
        "                          bench where only metabench drives traffic."
    )


# ---------- helpers ----------

def fmt_int(x: int | None) -> str:
    return "n/a" if x is None else f"{x}"


def fmt_float(x: float | None) -> str:
    return "n/a" if x is None else f"{x:.1f}"


def spawn_meta(binary: Path, cfg: Path, stdout_path: Path, cwd: Path) -> subprocess.Popen:
    f = open(stdout_path, "w")
    return subprocess.Popen(
        [str(binary), "-c", str(cfg)],
        cwd=cwd,
        stdout=f,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def wait_for_health(admin_port: int, name: str, timeout: float) -> None:
    url = f"http://127.0.0.1:{admin_port}/v1/health"
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with _http_get(url, timeout=1) as r:
                if 200 <= r.status < 300:
                    return
        except (urllib.error.URLError, ConnectionError, socket.timeout, OSError) as e:
            last_err = e
        time.sleep(0.3)
    raise RuntimeError(f"{name}: timeout waiting for {url} (last err: {last_err})")


def wait_for_voters(admin_port: int, expected: int, name: str, timeout: float) -> None:
    url = f"http://127.0.0.1:{admin_port}/v1/cluster/status"
    deadline = time.time() + timeout
    last: object = None
    while time.time() < deadline:
        try:
            with _http_get(url, timeout=1) as r:
                status = json.loads(r.read())
            last = status
            if extract_voter_count(status) >= expected:
                return
        except Exception as e:
            last = e
        time.sleep(0.3)
    raise RuntimeError(
        f"{name}: timeout waiting for {expected} voters via {url}; last: {last}"
    )


def extract_voter_count(status: object) -> int:
    if not isinstance(status, dict):
        return 0
    voters = status.get("voters")
    if isinstance(voters, list):
        return len(voters)
    metrics = status.get("metrics", {})
    if isinstance(metrics, dict):
        m = metrics.get("membership_config", {}).get("membership", {})
        if isinstance(m, dict):
            configs = m.get("configs", [])
            if isinstance(configs, list) and configs:
                return len(set().union(*configs))
    return 0


def parse_metabench_ms(line: str) -> int | None:
    """Extract milliseconds from `benchmark client(N) * number(M) in X milliseconds`."""
    parts = line.split(" in ")
    if len(parts) != 2:
        return None
    try:
        return int(parts[1].split(" ")[0])
    except ValueError:
        return None


def sum_count(log_dir: Path, needle: str) -> int:
    n = 0
    for f in log_dir.glob("databend-meta-*"):
        if not f.is_file():
            continue
        with open(f, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                n += chunk.count(needle.encode())
    return n


if __name__ == "__main__":
    sys.exit(main())
