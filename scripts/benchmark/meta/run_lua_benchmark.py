#!/usr/bin/env python3
"""Run a Lua benchmark script against a fresh meta service.

One node by default. A --config naming more nodes starts all of them, and the
script runs against whichever one is leader.

Run from the repository root:

    python scripts/benchmark/meta/run_lua_benchmark.py \
      scripts/benchmark/meta/lua/write_heavy_upsert_get.lua
"""

import argparse
import os
import sys
import time
from pathlib import Path

from cluster import Cluster
from cluster import ClusterSpec
from cluster import NodeSpec
from cluster import add_cluster_args
from cluster import check_paths_exist
from cluster import resolve_path_args
from cluster import run_logged
from cluster import spec_kwargs

BUILD_PROFILE = os.environ.get("BUILD_PROFILE", "debug")
DEFAULT_META_BIN = Path(f"./target/{BUILD_PROFILE}/databend-meta")

# One stamp per invocation: the work dir survives across invocations, and the
# stamp keeps every invocation's log from overwriting an earlier one's.
RUN_STAMP = time.strftime("%Y%m%d-%H%M%S")


def run_lua_script(metactl_bin, script_path, grpc_address, work_dir):
    """Run the Lua script with metactl, pointed at `grpc_address`."""
    print(f"[bench] grpc={grpc_address} script={script_path.name}", flush=True)

    # The address is injected as a Lua global before the script file is loaded:
    # metactl runs every --script and --file in one Lua state, in argument order.
    cmd = [
        str(metactl_bin),
        "lua",
        "--script",
        f'GRPC_ADDR = "{grpc_address}"',
        "--file",
        str(script_path),
    ]

    log_path = work_dir / f"lua-{RUN_STAMP}.log"
    run_logged(cmd, log_path, cwd=work_dir)
    print(f"[bench] log={log_path}", flush=True)


def parse_args():
    """Parse this runner's command line."""
    parser = argparse.ArgumentParser(
        description="Run a Lua benchmark against a databend-meta cluster.",
        epilog="Run from the repository root.",
    )
    parser.add_argument(
        "lua_script", type=Path, help="path to the Lua benchmark script"
    )
    parser.add_argument(
        "--meta-bin",
        type=Path,
        help="databend-meta to benchmark, used by every node; databend-metactl "
        "is taken from the same directory "
        "(default: ./target/$BUILD_PROFILE/databend-meta).",
    )
    add_cluster_args(parser)
    args = parser.parse_args()
    resolve_path_args(args, "meta_bin", "lua_script")
    return args


def build_spec(args) -> ClusterSpec:
    """Build the cluster spec, from --config and the CLI.

    With no config naming nodes, the run is one node on the default build.
    --meta-bin then renames the build of every node the config asked for
    rather than shrinking the cluster to one: this benchmark drives the leader
    whatever the cluster size, so there is nothing to compare builds against.
    """
    fields = spec_kwargs(args)
    config_nodes = fields.get("nodes")

    if config_nodes is None:
        meta_bin = args.meta_bin or DEFAULT_META_BIN.expanduser().resolve()
        fields["nodes"] = [NodeSpec(1, meta_bin)]
    elif args.meta_bin is not None:
        fields["nodes"] = [
            NodeSpec(node.node_id, args.meta_bin, node.label) for node in config_nodes
        ]

    return ClusterSpec(**fields)


def run_benchmark(args):
    """Start the cluster and run the Lua script against whichever node leads."""
    # Constructing the cluster starts nothing, so every input is checked before
    # a node boots. Cluster covers databend-meta; these two are what it cannot.
    cluster = Cluster(build_spec(args))
    metactl_bin = cluster.metactl_bin()
    check_paths_exist({"lua script": args.lua_script, "databend-metactl": metactl_bin})

    # Each node's own binary is printed by the launcher as it starts.
    print(f"[setup] nodes  = {len(cluster.spec.nodes)}", flush=True)
    print(f"[setup] script = {args.lua_script}", flush=True)
    print("", flush=True)

    with cluster:
        grpc_address = cluster.grpc_address()
        run_lua_script(metactl_bin, args.lua_script, grpc_address, cluster.work_dir)


def main() -> int:
    """Start a meta service, then run the given Lua benchmark on it."""
    args = parse_args()

    try:
        run_benchmark(args)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    print("[summary] benchmark completed", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
