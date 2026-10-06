#!/usr/bin/env python3
"""Start and control a local databend-meta cluster.

Node configs are generated from a ClusterSpec rather than read from preset
files, so a benchmark can change ports, raft dirs, and raft knobs without
editing a config file that other tooling shares.

A spec is built from a TOML file, from command-line options, or from both, the
options overriding the file.

The process lifecycle comes from the shared databend test helper. This module
adds benchmark configuration, command-line parsing, and workload execution.

    spec = ClusterSpec.uniform(Path("./target/debug/databend-meta"), node_count=3)
    with Cluster(spec) as cluster:
        run_my_workload(cluster.grpc_address())
"""

import argparse
import json
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "databend_test_helper" / "src"))
from databend_test_helper import (  # noqa: E402
    LocalMetaCluster,
    LocalMetaNode,
    MetaNodePorts,
)

DEFAULT_WORK_DIR = Path("./.databend")
DEFAULT_PORT_BASE = 28101

# A node's build directory holds the binaries; Cluster starts the first and
# runs seed import and transfer-leader through the second.
META_BIN_NAME = "databend-meta"
METACTL_BIN_NAME = "databend-metactl"

# Ports of one node are consecutive; the next node starts a stride later.
PORT_STRIDE = 100

TRANSFER_LEADER_TIMEOUT_SEC = 30

# Raft knobs every benchmark cluster pins, so numbers stay comparable across runs
# and across entry points. `snapshot_logs_since_last` decides what is measured:
# databend-meta snapshots every 1024 logs by default, which lands hundreds of
# snapshot generations inside a benchmark's write load. The two `snapshot_db_*`
# knobs repeat databend-meta's current defaults; they are pinned anyway because a
# version-comparison run gives each node a different build, and two builds need
# not agree on a default. ClusterSpec.raft_config overrides these entry by entry.
BENCHMARK_RAFT_CONFIG = {
    "install_snapshot_timeout": 60000,
    "max_applied_log_to_keep": 10240,
    "snapshot_db_block_keys": 8000,
    "snapshot_db_block_cache_size": 1073741824,
    "snapshot_logs_since_last": 1_000_000_000,
}

NODE_TOML = """\
admin_api_address       = "0.0.0.0:{admin_port}"
grpc_api_address        = "0.0.0.0:{grpc_port}"
grpc_api_advertise_host = "127.0.0.1"

[log]
[log.stderr]
on = false
[log.file]
on = true
level = "{log_level}"
format = "json"
dir = "{log_dir}"

[raft_config]
id            = {node_id}
raft_dir      = "{raft_dir}"
raft_api_port = {raft_port}
raft_listen_host    = "127.0.0.1"
raft_advertise_host = "localhost"
{bootstrap_mode}
{extra_raft_config}"""


@dataclass
class NodeSpec:
    """One meta node. `meta_bin` is per node so a run can compare two builds."""

    node_id: int
    meta_bin: Path
    label: str = ""


@dataclass
class ClusterSpec:
    """Everything needed to start one cluster: nodes, ports, dirs, and raft knobs."""

    nodes: list[NodeSpec]
    work_dir: Path = DEFAULT_WORK_DIR
    port_base: int = DEFAULT_PORT_BASE
    log_level: str = "INFO"

    # Wipe the work dir before starting. Off by default: benchmarks are often
    # run in sequence, each one measuring against the data its predecessors
    # left behind. Turn it on for a run that must start from empty state.
    reset_work_dir: bool = False

    # Overrides BENCHMARK_RAFT_CONFIG entry by entry, e.g. {"heartbeat_interval": 200}.
    raft_config: dict = field(default_factory=dict)

    # Exported meta data imported into every raft dir before the nodes start.
    seed_file: Path | None = None

    # Used for seed import and transfer-leader.
    # Defaults to databend-metactl next to the first node's databend-meta.
    metactl_bin: Path | None = None

    @classmethod
    def uniform(cls, meta_bin: Path, node_count: int = 3, **kwargs) -> "ClusterSpec":
        """Build a spec whose nodes all run the same binary."""
        nodes = [NodeSpec(node_id, meta_bin) for node_id in range(1, node_count + 1)]
        return cls(nodes=nodes, **kwargs)


class Cluster(LocalMetaCluster):
    """One local meta cluster, started and stopped as a context manager."""

    def __init__(self, spec: ClusterSpec) -> None:
        _validate_spec(spec)

        self.spec = spec
        raft_config = BENCHMARK_RAFT_CONFIG | spec.raft_config
        nodes = [self._local_node(node, raft_config) for node in spec.nodes]
        super().__init__(nodes, spec.work_dir, reset_work_dir=spec.reset_work_dir)

    def raft_dir(self, node_id: int) -> Path:
        return self.work_dir / f"raft{node_id}"

    # --- cluster state ---------------------------------------------------

    def transfer_leader_to(self, node_id: int) -> None:
        """Move leadership to `node_id`, then wait until the move is visible."""
        current = self.leader_id()
        if current == node_id:
            return

        cmd = [
            str(self.metactl_bin()),
            "transfer-leader",
            "--to",
            str(node_id),
            "--admin-api-address",
            self.admin_address(current),
        ]
        subprocess.run(cmd, check=True)

        self._wait_until(
            lambda: self._leader_id_or_none() == node_id,
            self.node_ids,
            f"the leader to move to node{node_id}",
            timeout=TRANSFER_LEADER_TIMEOUT_SEC,
        )

    # --- start() steps ---------------------------------------------------

    def _prepare_data(self) -> None:
        if self.spec.seed_file is not None:
            self._import_seed()

    def _import_seed(self) -> None:
        """Load the seed export into every node's raft dir before the nodes start."""
        initial_cluster = [
            f"{node_id}=localhost:{self.raft_port(node_id)}"
            for node_id in self.node_ids
        ]
        for node_id in self.node_ids:
            cmd = [
                str(self.metactl_bin()),
                "import",
                "--raft-dir",
                str(self.raft_dir(node_id)),
                "--id",
                str(node_id),
                "--db",
                str(self.spec.seed_file),
            ]
            for member in initial_cluster:
                cmd.extend(["--initial-cluster", member])

            print(f"[seed] import node{node_id}", flush=True)
            subprocess.run(cmd, check=True)

    def _local_node(self, node: NodeSpec, raft_config: dict) -> LocalMetaNode:
        """Describe one node to the shared lifecycle: ports, files, and config."""
        node_id = node.node_id
        return LocalMetaNode(
            node_id=node_id,
            meta_bin=node.meta_bin,
            ports=self._node_ports(node_id),
            config_path=Path(f"node{node_id}.toml"),
            config_text=self._render_config(node_id, raft_config),
            stdout_path=Path(f"node{node_id}.stdout.log"),
            label=node.label,
            wait_for_voter=self.spec.seed_file is None,
        )

    def _node_ports(self, node_id: int) -> MetaNodePorts:
        admin_port = self.spec.port_base + (node_id - 1) * PORT_STRIDE
        return MetaNodePorts(admin=admin_port, grpc=admin_port + 1, raft=admin_port + 2)

    def _render_config(self, node_id: int, raft_config: dict) -> str:
        """Render one node's config file."""
        first_id = self.spec.nodes[0].node_id
        if node_id == first_id:
            bootstrap_mode = "single = true"
        else:
            first_raft_port = self._node_ports(first_id).raft
            bootstrap_mode = f'join = ["127.0.0.1:{first_raft_port}"]'

        ports = self._node_ports(node_id)
        return NODE_TOML.format(
            node_id=node_id,
            admin_port=ports.admin,
            grpc_port=ports.grpc,
            raft_port=ports.raft,
            raft_dir=f"raft{node_id}",
            log_dir=f"logs{node_id}",
            log_level=self.spec.log_level,
            bootstrap_mode=bootstrap_mode,
            extra_raft_config=_render_raft_config(raft_config),
        )

    # --- helpers ---------------------------------------------------------

    def metactl_bin(self) -> Path:
        """The databend-metactl that goes with this cluster's nodes."""
        if self.spec.metactl_bin is not None:
            return self.spec.metactl_bin.expanduser().resolve()

        first_meta_bin = self.spec.nodes[0].meta_bin.expanduser().resolve()
        return first_meta_bin.parent / METACTL_BIN_NAME


def run_logged(cmd: list[str], log_path: Path, cwd: Path | None = None) -> None:
    """Run a workload, echoing its output live and keeping a copy in `log_path`.

    A benchmark that only wrote a log looked hung for its whole run; one that
    only printed left nothing to re-read afterwards.
    """
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    with log_path.open("w") as log_file:
        for output_line in process.stdout:
            print(output_line, end="", flush=True)
            log_file.write(output_line)

    returncode = process.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, cmd)


def add_cluster_args(parser: argparse.ArgumentParser) -> None:
    """Add the cluster options every benchmark entry point offers."""
    parser.add_argument(
        "--config",
        type=Path,
        help="TOML file describing the cluster; every option below overrides "
        "what it sets. Paths inside it resolve against its own directory.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="scratch dir for node configs, logs, and raft state "
        f"(default: {DEFAULT_WORK_DIR}).",
    )
    parser.add_argument(
        "--port-base",
        type=int,
        help="node N listens on admin port_base+(N-1)*100, grpc +1, raft +2; "
        "give two benchmarks running side by side different bases "
        f"(default: {DEFAULT_PORT_BASE}).",
    )
    parser.add_argument(
        "--reset-work-dir",
        action="store_true",
        default=None,
        help="wipe the work dir before starting; by default the run keeps "
        "whatever earlier runs left there, so workloads can accumulate.",
    )


def spec_kwargs(args: argparse.Namespace) -> dict:
    """ClusterSpec fields from --config, overridden by the ones the CLI set.

    A field the CLI left unset stays absent rather than arriving as None, so
    ClusterSpec's own defaults still apply to whatever neither source named.
    """
    config_fields = load_cluster_toml(args.config) if args.config is not None else {}

    cli_values = {
        "work_dir": args.work_dir,
        "port_base": args.port_base,
        "reset_work_dir": args.reset_work_dir,
    }
    cli_fields = {
        field_name: value
        for field_name, value in cli_values.items()
        if value is not None
    }
    return config_fields | cli_fields


# Keys a cluster TOML file may set; each one names a ClusterSpec field.
# Path-valued keys resolve against the config file's own directory.
CLUSTER_TOML_PLAIN_KEYS = ("port_base", "log_level", "reset_work_dir", "raft_config")
CLUSTER_TOML_PATH_KEYS = ("work_dir", "seed_file", "metactl_bin")
CLUSTER_TOML_KEYS = {"nodes", *CLUSTER_TOML_PLAIN_KEYS, *CLUSTER_TOML_PATH_KEYS}


def load_cluster_toml(config_path: Path) -> dict:
    """Read a cluster TOML file into ClusterSpec fields.

    Its paths resolve against the file's own directory, so a config travels
    with the build tree it points at rather than with the caller's cwd.
    """
    with config_path.open("rb") as config_file:
        config_data = tomllib.load(config_file)

    unknown_keys = sorted(set(config_data) - CLUSTER_TOML_KEYS)
    if unknown_keys:
        known_keys = sorted(CLUSTER_TOML_KEYS)
        raise ValueError(
            f"{config_path}: unknown keys {unknown_keys}; known keys are {known_keys}"
        )

    config_dir = config_path.parent.expanduser().resolve()
    spec_fields = {}

    for plain_key in CLUSTER_TOML_PLAIN_KEYS:
        if plain_key in config_data:
            spec_fields[plain_key] = config_data[plain_key]

    for path_key in CLUSTER_TOML_PATH_KEYS:
        if path_key in config_data:
            spec_fields[path_key] = _resolve_config_path(
                config_data[path_key], config_dir
            )

    if "nodes" in config_data:
        spec_fields["nodes"] = _load_node_table(
            config_data["nodes"], config_dir, config_path
        )

    return spec_fields


def build_nodes(bin_dirs: list[Path], labels: list | None = None) -> list[NodeSpec]:
    """One node per build directory, numbered in order.

    A node's label names its build in the report, and defaults to the build
    directory's own name.
    """
    if labels and len(labels) != len(bin_dirs):
        raise ValueError(
            f"got {len(labels)} labels for {len(bin_dirs)} nodes; "
            "give one label per node, or none at all"
        )

    nodes = []
    for index, bin_dir in enumerate(bin_dirs):
        given_label = labels[index] if labels else None
        node = NodeSpec(index + 1, bin_dir / META_BIN_NAME, given_label or bin_dir.name)
        nodes.append(node)
    return nodes


def _load_node_table(
    node_entries: list, config_dir: Path, config_path: Path
) -> list[NodeSpec]:
    """Build node specs from the `[[nodes]]` tables of a cluster TOML file.

    One table describes a group of identical nodes rather than a single node,
    so a uniform cluster of any size is one table with `count`, and a version
    comparison is one table per build.
    """
    if not node_entries:
        raise ValueError(f"{config_path}: `nodes` is empty")

    bin_dirs = []
    labels = []
    for index, node_entry in enumerate(node_entries):
        position = index + 1
        unknown_keys = sorted(set(node_entry) - {"bin", "label", "count"})
        if unknown_keys:
            raise ValueError(
                f"{config_path}: [[nodes]] {position} has unknown keys {unknown_keys}"
            )
        if "bin" not in node_entry:
            raise ValueError(f"{config_path}: [[nodes]] {position} has no `bin`")

        group_size = node_entry.get("count", 1)
        check_node_count(group_size, f"{config_path}: [[nodes]] {position} count")

        group_bin_dir = _resolve_config_path(node_entry["bin"], config_dir)
        group_label = node_entry.get("label")
        bin_dirs.extend([group_bin_dir] * group_size)
        labels.extend([group_label] * group_size)

    return build_nodes(bin_dirs, labels)


def check_node_count(count, what: str) -> None:
    """Reject a node count that is not a positive whole number.

    `what` names the source in the message, so the caller's own spelling --
    a config position or a command-line flag -- reaches the user.
    """
    # bool is an int subclass, so `count = true` would otherwise pass as 1.
    is_whole_number = isinstance(count, int) and not isinstance(count, bool)
    if not is_whole_number or count < 1:
        raise ValueError(f"{what} must be a positive integer, got {count!r}")


def _resolve_config_path(raw_path: str, config_dir: Path) -> Path:
    """Resolve one path read from a config file against that file's directory."""
    expanded_path = Path(raw_path).expanduser()
    if expanded_path.is_absolute():
        return expanded_path
    return (config_dir / expanded_path).resolve()


def resolve_path_args(args: argparse.Namespace, *arg_names: str) -> None:
    """Make the named path arguments absolute; an unset optional one stays None.

    A list-valued argument has every element resolved.
    """
    for arg_name in arg_names:
        arg_value = getattr(args, arg_name)
        if arg_value is None:
            continue
        if isinstance(arg_value, list):
            resolved = [one_path.expanduser().resolve() for one_path in arg_value]
            setattr(args, arg_name, resolved)
        else:
            setattr(args, arg_name, arg_value.expanduser().resolve())


def check_paths_exist(required_paths: dict[str, Path]) -> None:
    """Reject missing inputs, naming every one of them rather than the first."""
    missing_paths = [
        f"{label}: {required_path}"
        for label, required_path in required_paths.items()
        if not required_path.exists()
    ]
    if missing_paths:
        joined = "\n  ".join(missing_paths)
        raise FileNotFoundError(f"missing required paths:\n  {joined}")


def _validate_spec(spec: ClusterSpec) -> None:
    """Reject a spec that cannot produce a working cluster."""
    node_ids = [node.node_id for node in spec.nodes]
    if not node_ids:
        raise ValueError("cluster needs at least one node")

    expected_ids = list(range(1, len(node_ids) + 1))
    if node_ids != expected_ids:
        raise ValueError(f"node ids must be {expected_ids}, got {node_ids}")

    for node in spec.nodes:
        meta_bin = node.meta_bin.expanduser()
        if "debug" in meta_bin.parts:
            print(
                f"[warn] node{node.node_id} runs a debug build ({meta_bin}); "
                "its numbers do not reflect release performance",
                file=sys.stderr,
            )

    if spec.seed_file is not None and not spec.seed_file.expanduser().exists():
        raise FileNotFoundError(f"seed file: {spec.seed_file}")


def _render_raft_config(raft_config: dict) -> str:
    """Render the spec's extra raft knobs as `[raft_config]` lines."""
    knob_lines = [
        f"{knob_name} = {_toml_literal(knob_value)}"
        for knob_name, knob_value in raft_config.items()
    ]
    return "".join(f"{knob_line}\n" for knob_line in knob_lines)


def _toml_literal(python_value) -> str:
    """Render one Python value as the TOML literal that stands for it."""
    if isinstance(python_value, bool):
        return "true" if python_value else "false"
    if isinstance(python_value, (int, float)):
        return str(python_value)
    if isinstance(python_value, str):
        return json.dumps(python_value)
    raise TypeError(f"unsupported raft_config value: {python_value!r}")
