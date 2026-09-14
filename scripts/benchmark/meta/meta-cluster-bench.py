#!/usr/bin/env python3
"""Multi-node databend-meta cluster benchmark.

Starts one leader and its followers, drives load through the leader (node 1
unless --leader picks another) with databend-metabench, and prints the
metabench summary.

--seed-file imports the same exported meta data into every raft dir before the
nodes start, so the workload measures against real state; without it the
cluster starts empty and the report labels the run "unseeded". The seed file
is used as-is: prepare or sanitize it before invoking this script.

Example:

    scripts/benchmark/meta/meta-cluster-bench.py \
      --node-bin ~/bin/v1.2.819-nightly ~/bin/v1.2.819-nightly ~/bin/v1.2.908-nightly \
      --node-label 819 819 908 \
      --seed-file ~/data/backup.data

The same run from a config file:

    scripts/benchmark/meta/meta-cluster-bench.py --config my-cluster.toml
"""

import argparse
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from cluster import Cluster
from cluster import ClusterSpec
from cluster import add_cluster_args
from cluster import build_nodes
from cluster import check_node_count
from cluster import check_paths_exist
from cluster import resolve_path_args
from cluster import run_logged
from cluster import spec_kwargs

# The load generator, taken from node1's build directory.
BENCH_BIN_NAME = "databend-metabench"

# One stamp per invocation: the work dir survives across invocations, and the
# stamp keeps every invocation's logs from overwriting an earlier one's.
RUN_STAMP = time.strftime("%Y%m%d-%H%M%S")


@dataclass
class RunResult:
    workload_ops: int
    # The `key=value` metrics of metabench's summary line, kept as printed.
    metrics: dict[str, str] = field(default_factory=dict)
    # Each node's self-reported binary version, captured while the cluster ran.
    node_versions: dict[int, str] = field(default_factory=dict)
    latency_histogram: str | None = None
    # Set when the run produced no usable measurement; decides the exit code.
    failed: bool = False
    notes: list[str] = field(default_factory=list)

    def metric(self, key: str) -> str:
        """One summary metric, or `n/a` when the run did not measure it."""
        return self.metrics.get(key, "n/a")

    def metric_pair(self, key_a: str, key_b: str) -> str:
        """Two related metrics as one `a/b` cell, `n/a` unless both are present."""
        if key_a in self.metrics and key_b in self.metrics:
            return f"{self.metrics[key_a]}/{self.metrics[key_b]}"
        return "n/a"


def main() -> int:
    """Run one benchmark over the configured cluster and print its report."""
    args = parse_args()

    # Constructing the cluster starts nothing, so a bad config, a missing
    # binary, or a missing seed file costs nothing but the error message.
    # Cluster validates the meta binaries and the seed file; the two binaries
    # checked here are the ones it does not know about.
    try:
        cluster = Cluster(build_spec(args))
        bench_bin = cluster.spec.nodes[0].meta_bin.parent / BENCH_BIN_NAME
        check_paths_exist(
            {
                "node1 databend-metabench": bench_bin,
                "databend-metactl": cluster.metactl_bin(),
            }
        )
        if args.leader not in cluster.node_ids:
            raise ValueError(
                f"--leader {args.leader} is not a node; ids are {cluster.node_ids}"
            )
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    results = run_cluster(cluster, bench_bin, args)
    print_report(results, cluster, args)

    if any(result.failed for result in results):
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    """Parse this benchmark's command line."""
    parser = argparse.ArgumentParser(
        description="Run one multi-node databend-meta benchmark.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--node-bin",
        type=Path,
        nargs="+",
        metavar="DIR",
        help="one build directory per node, each holding databend-meta; "
        "name different builds to compare them in a single run.",
    )
    parser.add_argument(
        "--node-label",
        nargs="+",
        metavar="LABEL",
        help="one report label per node, in --node-bin order "
        "(default: the build directory's own name).",
    )
    parser.add_argument(
        "--node-count",
        type=int,
        metavar="N",
        help="run N nodes off a single --node-bin, instead of naming that "
        "build once per node.",
    )
    parser.add_argument(
        "--seed-file",
        type=Path,
        help="exported meta data file; used as-is by databend-metactl import. "
        "It replaces raft state whether or not --reset-work-dir is given.",
    )
    parser.add_argument(
        "--metactl-bin",
        type=Path,
        help="databend-metactl used for import and transfer-leader "
             "(default: the one beside node1's databend-meta).",
    )
    parser.add_argument("--workload-clients", type=int, default=4)
    parser.add_argument("--workload-client-pool-size", type=int, default=1)
    parser.add_argument("--workload-ops", type=int, default=10000)
    parser.add_argument("--workload-rpc", default="upsert_kv")
    parser.add_argument(
        "--workload-run-secs",
        type=int,
        help="cap each run at this many seconds of wall clock (metabench "
        "--run-secs); --workload-ops still bounds how much each client issues.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        metavar="N",
        help="run the workload N times in one cluster session; the report "
        "lists every run's qps and its table shows the median-qps run.",
    )
    parser.add_argument(
        "--leader",
        type=int,
        default=1,
        metavar="N",
        help="node given leadership before the workload runs; the workload "
        "drives this node, so it decides which build leads a mixed-build "
        "cluster (default: 1).",
    )
    add_cluster_args(parser)
    parser.add_argument(
        "--snapshot-logs-since-last",
        type=int,
        help="raise to keep snapshot generation out of the measured window, "
        "lower to measure it (default: the benchmark baseline in cluster.py).",
    )
    parser.add_argument("--post-bench-wait-secs", type=float, default=2.0)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error(f"--repeat must be a positive integer, got {args.repeat}")
    resolve_path_args(args, "node_bin", "seed_file", "metactl_bin")
    return args


def build_spec(args: argparse.Namespace) -> ClusterSpec:
    """Build the cluster spec this run measures, from --config and the CLI."""
    fields = spec_kwargs(args)

    if args.node_bin is not None:
        bin_dirs, labels = repeat_single_build(
            args.node_bin, args.node_label, args.node_count
        )
        fields["nodes"] = build_nodes(bin_dirs, labels)
    elif args.node_label is not None:
        raise ValueError("--node-label needs --node-bin; a config file labels "
                         "its own nodes with `label` inside [[nodes]]")
    elif args.node_count is not None:
        raise ValueError("--node-count needs --node-bin; a config file sizes "
                         "its own node groups with `count` inside [[nodes]]")

    for name in ("seed_file", "metactl_bin"):
        value = getattr(args, name)
        if value is not None:
            fields[name] = value

    if args.snapshot_logs_since_last is not None:
        override = {"snapshot_logs_since_last": args.snapshot_logs_since_last}
        fields["raft_config"] = fields.get("raft_config", {}) | override

    if "nodes" not in fields:
        raise ValueError("no nodes: pass --node-bin, or [[nodes]] in --config")

    return ClusterSpec(**fields)


def repeat_single_build(bin_dirs: list[Path], labels, node_count) -> tuple[list, list]:
    """Grow one --node-bin into --node-count identical nodes."""
    if node_count is None:
        return bin_dirs, labels

    check_node_count(node_count, "--node-count")
    if len(bin_dirs) != 1:
        raise ValueError(
            f"--node-count {node_count} takes exactly one --node-bin to repeat, "
            f"got {len(bin_dirs)}"
        )
    if labels is not None and len(labels) != 1:
        raise ValueError(
            f"--node-count {node_count} takes at most one --node-label to repeat, "
            f"got {len(labels)}"
        )

    repeated_labels = labels * node_count if labels else None
    return bin_dirs * node_count, repeated_labels


def run_cluster(
    cluster: Cluster,
    bench_bin: Path,
    args: argparse.Namespace,
) -> list[RunResult]:
    """Run the workload --repeat times against a freshly started cluster.

    The runs share the cluster and its accumulating data, exactly like
    successive invocations of this script do. A failed run ends the sequence.
    """
    results = []

    try:
        print_setup(cluster, bench_bin)

        with cluster:
            node_versions = cluster.binary_versions()
            cluster.transfer_leader_to(args.leader)

            for run_index in range(1, args.repeat + 1):
                result = RunResult(
                    workload_ops=args.workload_clients * args.workload_ops,
                    node_versions=node_versions,
                )
                results.append(result)

                run_metabench(cluster, bench_bin, args, run_index)
                parse_metabench(bench_log_path(cluster, run_index), result)
                print(
                    f"[summary] run {run_index}/{args.repeat} "
                    f"qps={result.metric('qps')} "
                    f"elapsed_ms={result.metric('elapsed_ms')}"
                )
                if result.failed:
                    break

            if args.post_bench_wait_secs > 0:
                time.sleep(args.post_bench_wait_secs)
    except Exception as e:
        if not results:
            results.append(
                RunResult(workload_ops=args.workload_clients * args.workload_ops)
            )
        results[-1].failed = True
        results[-1].notes.append(f"error: {e}")
        print(f"[ERROR] {e}", file=sys.stderr)

    return results


def print_setup(cluster: Cluster, bench_bin: Path) -> None:
    """Print the binaries, ports, and paths this run will use."""
    for node in cluster.spec.nodes:
        node_id = node.node_id
        admin = cluster.admin_port(node_id)
        grpc = cluster.grpc_port(node_id)
        raft = cluster.raft_port(node_id)
        ports = f"{admin}/{grpc}/{raft}"
        print(
            f"[setup] node{node_id} {node.label} -> {node.meta_bin} "
            f"ports admin/grpc/raft={ports}",
            flush=True,
        )
    print(f"[setup] seed    = {cluster.spec.seed_file or 'unseeded'}", flush=True)
    print(f"[setup] bench   = {bench_bin}", flush=True)
    print(f"[setup] workdir = {cluster.work_dir}", flush=True)
    print("", flush=True)


def bench_log_path(cluster: Cluster, run_index: int) -> Path:
    """Where run number `run_index` of metabench logs its output."""
    return cluster.work_dir / f"metabench-{RUN_STAMP}.{run_index}.log"


def run_metabench(
    cluster: Cluster,
    bench_bin: Path,
    args: argparse.Namespace,
    run_index: int,
) -> None:
    """Drive the workload through the leader node with databend-metabench."""
    bench_log = bench_log_path(cluster, run_index)
    grpc_address = cluster.grpc_address(args.leader)
    print(
        f"[bench] grpc={grpc_address} "
        f"client={args.workload_clients} "
        f"number={args.workload_ops} "
        f"client_pool_size={args.workload_client_pool_size} "
        f"rpc={args.workload_rpc}",
        flush=True,
    )

    cmd = [
        str(bench_bin),
        "--grpc-api-address",
        grpc_address,
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
    ]
    if args.workload_run_secs is not None:
        cmd.extend(["--run-secs", str(args.workload_run_secs)])
    run_logged(cmd, bench_log, cwd=cluster.work_dir)
    print(f"[bench] log={bench_log}", flush=True)


def parse_metabench(log_path: Path, result: RunResult) -> None:
    """Fill `result` with the metrics metabench left in its log.

    Any metabench this script can run prints the summary line: the line and
    the --client-pool-size flag the script always passes entered metabench
    together (v1.2.911), so an older build fails before producing output.
    """
    lines = log_path.read_text(errors="replace").splitlines()

    summary_line = find_line(lines, "benchmark summary:")
    if summary_line is None:
        result.failed = True
        result.notes.append("metabench produced no summary line")
        return

    result.metrics = parse_key_value_line(summary_line, "benchmark summary:")

    histogram_line = find_line(lines, "benchmark latency histogram:")
    if histogram_line is not None:
        result.latency_histogram = histogram_line.split(":", 1)[1].strip()


def print_report(
    results: list[RunResult],
    cluster: Cluster,
    args: argparse.Namespace,
) -> None:
    """Print the final report; with --repeat, the table shows the median-qps run."""
    measured = [one_result for one_result in results if "qps" in one_result.metrics]
    if measured:
        ranked = sorted(measured, key=lambda r: float(r.metrics["qps"]))
        result = ranked[len(ranked) // 2]
    else:
        result = results[-1]

    print(f"\n{'=' * 72}\n=== Report\n{'=' * 72}\n")
    for node in cluster.spec.nodes:
        node_id = node.node_id
        version = result.node_versions.get(node_id, "version unknown")
        print(
            f"node{node_id}: {node.label} {version} "
            f"(grpc={cluster.grpc_address(node_id)}, "
            f"raft=localhost:{cluster.raft_port(node_id)})"
        )
    # metabench reports the effective pool size; before the run it is the ask.
    pool_size = result.metrics.get("client_pool_size", args.workload_client_pool_size)
    print(
        f"workload: {args.workload_clients} clients x {args.workload_ops} "
        f"ops/client = {result.workload_ops} ops via {args.workload_rpc}; "
        f"client_pool_size={pool_size}"
    )
    print(f"seed    : {cluster.spec.seed_file or 'unseeded'}")
    print(f"work_dir: {cluster.work_dir}\n")

    if len(results) > 1:
        per_run = "  ".join(
            f"run{run_number}={one_result.metric('qps')}"
            for run_number, one_result in enumerate(results, 1)
        )
        print(f"qps per run: {per_run}")
        if measured:
            qps_values = sorted(float(r.metrics["qps"]) for r in measured)
            print(
                f"qps median={result.metric('qps')} "
                f"min={qps_values[0]} max={qps_values[-1]}; "
                f"the table below shows the median run"
            )
        print()

    rows = [
        ("throughput (ops/sec)", result.metric("qps")),
        ("success qps", result.metric("success_qps")),
        ("error qps", result.metric("error_qps")),
        ("wall (ms)", result.metric("elapsed_ms")),
        ("metabench total", result.metric("total")),
        ("metabench success", result.metric("success")),
        ("metabench error", result.metric("error")),
        ("latency avg/max (us)", result.metric_pair("avg_us", "max_us")),
        ("latency p50/p90 (us)", result.metric_pair("p50_us", "p90_us")),
        ("latency p95/p99 (us)", result.metric_pair("p95_us", "p99_us")),
    ]
    label_w = max(len(label) for label, _ in rows)
    for label, value in rows:
        print(f"  {label.ljust(label_w)}  {value}")

    if result.latency_histogram:
        print("\nlatency histogram:")
        print(f"  {result.latency_histogram}")

    all_notes = []
    for run_number, one_result in enumerate(results, 1):
        run_tag = f"run{run_number}: " if len(results) > 1 else ""
        all_notes.extend(run_tag + note for note in one_result.notes)
    if all_notes:
        print("\nnotes:")
        for note in all_notes:
            print(f"  - {note}")


def find_line(lines: list[str], prefix: str) -> str | None:
    """The first line starting with `prefix`, or None when there is none."""
    return next((line for line in lines if line.startswith(prefix)), None)


def parse_key_value_line(line: str, prefix: str) -> dict[str, str]:
    """Split the `key=value ...` metrics after `prefix` into a dict."""
    metrics = {}
    for part in line[len(prefix):].split():
        if "=" in part:
            key, value = part.split("=", 1)
            metrics[key] = value
    return metrics


if __name__ == "__main__":
    sys.exit(main())
