"""Start, stop, and restart the databend-meta processes of one local cluster.

The caller describes each node up front: its binary, its ports, and the full
text of its config file. This module owns the rest: the work dir, the log
files, readiness, teardown, and restarting a single node.

    with LocalMetaCluster(nodes, work_dir) as cluster:
        run_against(cluster.grpc_address())
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

START_TIMEOUT_SEC = 90
STOP_TIMEOUT_SEC = 8
POLL_INTERVAL_SEC = 0.5
HTTP_TIMEOUT_SEC = 1
PORT_PROBE_TIMEOUT_SEC = 0.3

_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


@dataclass(frozen=True)
class MetaNodePorts:
    """Listener ports of one node."""

    admin: int
    grpc: int
    raft: int
    raft_tls: int | None = None

    @property
    def all(self) -> tuple[int, ...]:
        """Every port the node listens on."""
        if self.raft_tls is None:
            return (self.admin, self.grpc, self.raft)
        return (self.admin, self.grpc, self.raft, self.raft_tls)


@dataclass(frozen=True)
class LocalMetaNode:
    """One databend-meta process: its binary, ports, and generated config.

    `config_path` and `stdout_path` are relative to the cluster work dir,
    which is also the process's cwd.
    """

    node_id: int
    meta_bin: Path
    ports: MetaNodePorts
    config_path: Path
    config_text: str = field(repr=False)
    stdout_path: Path
    label: str = ""

    # Seeded nodes already carry the full membership, so none of them joins
    # and there is no membership change to wait for. Waiting would deadlock:
    # a seeded node1 alone cannot elect a leader.
    wait_for_voter: bool = True


class LocalMetaCluster:
    """One local meta cluster, started and stopped as a context manager.

    `reset_work_dir` wipes the work dir before starting.
    `cleanup_work_dir_on_success` deletes it when the `with` block succeeds and
    keeps it when the block fails. `start_timeout` bounds each wait: a node
    answering health, a node joining, the cluster forming.
    """

    def __init__(
        self,
        nodes: list[LocalMetaNode],
        work_dir: Path,
        *,
        reset_work_dir: bool = False,
        cleanup_work_dir_on_success: bool = False,
        start_timeout: float = START_TIMEOUT_SEC,
    ) -> None:
        _validate_nodes(nodes)

        self.work_dir = work_dir.expanduser().resolve()
        self.node_ids = [node.node_id for node in nodes]
        self._nodes = {node.node_id: node for node in nodes}
        self._reset_work_dir = reset_work_dir
        self._cleanup_work_dir_on_success = cleanup_work_dir_on_success
        self._start_timeout = start_timeout
        self._procs: dict[int, subprocess.Popen] = {}

    # --- addresses -------------------------------------------------------

    def admin_port(self, node_id: int) -> int:
        return self._nodes[node_id].ports.admin

    def grpc_port(self, node_id: int) -> int:
        return self._nodes[node_id].ports.grpc

    def raft_port(self, node_id: int) -> int:
        return self._nodes[node_id].ports.raft

    def admin_address(self, node_id: int) -> str:
        return f"127.0.0.1:{self.admin_port(node_id)}"

    def grpc_address(self, node_id: int | None = None) -> str:
        """gRPC address of `node_id`, or of the current leader when omitted."""
        target_node = self.leader_id() if node_id is None else node_id
        return f"127.0.0.1:{self.grpc_port(target_node)}"

    # --- lifecycle -------------------------------------------------------

    def __enter__(self) -> "LocalMetaCluster":
        self.start()
        return self

    def __exit__(self, exc_type, _exc_value, _traceback) -> None:
        self.stop()
        if not self._cleanup_work_dir_on_success:
            return
        if exc_type is not None:
            print(f"[workdir] kept for inspection: {self.work_dir}", flush=True)
            return
        shutil.rmtree(self.work_dir)

    def start(self) -> None:
        """Bring up every node and return once the cluster has elected a leader.

        Nodes start one at a time, each fully joined before the next. A node
        configured with `join` sends an add-node request as soon as it boots.
        Starting them together makes a second request arrive while the first
        membership change is still in flight, which the leader rejects with
        "the cluster is already undergoing a configuration change", taking
        down the whole startup.
        """
        self._check_ports_free()
        self._prepare_work_dir()
        self._prepare_data()
        self._write_configs()

        try:
            for node in self._nodes.values():
                self._start_node(node)
            self._wait_cluster()
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        """Terminate every node this cluster started."""
        for node_id in reversed(list(self._procs)):
            self.stop_node(node_id)

    def stop_node(self, node_id: int) -> None:
        """Terminate one running node."""
        node_process = self._procs.pop(node_id)
        _terminate(node_process)
        print(
            f"[teardown] node{node_id}: exited (rc={node_process.returncode})",
            flush=True,
        )

    def start_node(self, node_id: int) -> None:
        """Start a node stopped with `stop_node()` again, then wait until it has rejoined."""
        self._start_node(self._nodes[node_id], append_log=True)

    def restart_node(self, node_id: int) -> None:
        """Stop one node and start it again, then wait until it has rejoined."""
        self.stop_node(node_id)
        self.start_node(node_id)

    # --- cluster state ---------------------------------------------------

    def status(self, node_id: int | None = None) -> dict:
        """Cluster status as seen by `node_id`, or by the first node when omitted."""
        target_node = self.node_ids[0] if node_id is None else node_id
        status_url = f"http://{self.admin_address(target_node)}/v1/cluster/status"
        with _http_get(status_url) as response:
            return json.loads(response.read())

    def binary_versions(self) -> dict[int, str]:
        """Each node's self-reported binary version, keyed by node id.

        Only works while the cluster runs; capture it before teardown.
        """
        versions = {}
        for node_id in self.node_ids:
            node_status = self.status(node_id)
            versions[node_id] = node_status.get("binary_version", "unknown")
        return versions

    def leader_id(self) -> int:
        """Node id of the current leader; raises when the cluster has none."""
        leader = _extract_leader(self.status())
        if leader is None:
            raise RuntimeError("cluster has no leader")
        return leader

    def _leader_id_or_none(self) -> int | None:
        """leader_id() for polling loops: None while the cluster is not answering."""
        status = _swallow_transient(self.status)
        if status is None:
            return None
        return _extract_leader(status)

    # --- start() steps ---------------------------------------------------

    def _check_ports_free(self) -> None:
        """Fail before touching the work dir, so a refused start destroys nothing."""
        all_ports = _all_ports(self._nodes.values())
        occupied_ports = [port for port in all_ports if _port_in_use(port)]
        if occupied_ports:
            port_list = ", ".join(str(port) for port in occupied_ports)
            raise RuntimeError(f"ports already in use: {port_list}")

    def _prepare_work_dir(self) -> None:
        """Create the work dir, wiping any earlier run's data only when asked."""
        if self._reset_work_dir:
            print(f"[workdir] reset {self.work_dir}", flush=True)
            shutil.rmtree(self.work_dir, ignore_errors=True)

        self.work_dir.mkdir(parents=True, exist_ok=True)
        for node in self._nodes.values():
            for path in (node.config_path, node.stdout_path):
                (self.work_dir / path).parent.mkdir(parents=True, exist_ok=True)

    def _prepare_data(self) -> None:
        """Hook for a subclass to fill the raft dirs before the nodes start."""

    def _write_configs(self) -> None:
        """Write one config file per node into the work dir."""
        for node in self._nodes.values():
            config_path = self.work_dir / node.config_path
            config_path.write_text(node.config_text)

    def _start_node(self, node: LocalMetaNode, append_log: bool = False) -> None:
        """Launch one node and wait until it answers and, by default, has joined."""
        label_suffix = f" {node.label}" if node.label else ""
        print(f"[start] node{node.node_id}{label_suffix} {node.meta_bin}", flush=True)
        self._procs[node.node_id] = self._spawn(node, append_log)

        self._wait_health(node.node_id)
        if node.wait_for_voter:
            self._wait_voter(node.node_id)

    def _spawn(self, node: LocalMetaNode, append_log: bool) -> subprocess.Popen:
        """Launch one node process."""
        stdout_path = self.work_dir / node.stdout_path
        meta_bin = node.meta_bin.expanduser().resolve()
        cmd = [str(meta_bin), "-c", str(self.work_dir / node.config_path)]

        with stdout_path.open("a" if append_log else "w") as stdout_file:
            return subprocess.Popen(
                cmd,
                cwd=self.work_dir,
                stdout=stdout_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

    def _wait_health(self, node_id: int) -> None:
        """Wait until `node_id` answers its health endpoint."""
        health_url = f"http://{self.admin_address(node_id)}/v1/health"
        self._wait_until(lambda: _http_ok(health_url), [node_id], health_url)

    def _wait_voter(self, node_id: int) -> None:
        """Wait until `node_id` sees itself as a committed voter with a leader."""
        what = f"node{node_id} to join the cluster membership"
        self._wait_until(lambda: self._is_voter(node_id), [node_id], what)

    def _is_voter(self, node_id: int) -> bool:
        status = _swallow_transient(self.status, node_id)
        if status is None:
            return False
        joined = node_id in _extract_voters(status)
        elected = _extract_leader(status) is not None
        return joined and elected

    def _wait_cluster(self) -> None:
        """Wait until some node reports full membership with a leader, then print it."""
        what = f"a {len(self.node_ids)}-node cluster"
        status = self._wait_until(self._settled_status, self.node_ids, what)

        voters = sorted(_extract_voters(status))
        print(f"[cluster] leader={_extract_leader(status)} voters={voters}", flush=True)

    def _settled_status(self) -> dict | None:
        """The status of the first node that reports full membership and a leader."""
        for node_id in self.node_ids:
            status = _swallow_transient(self.status, node_id)
            if status is None:
                continue
            complete = len(_extract_voters(status)) >= len(self.node_ids)
            if complete and _extract_leader(status) is not None:
                return status
        return None

    def _wait_until(
        self, ready, watched_nodes, what: str, timeout: float | None = None
    ):
        """Poll `ready()` until it returns a truthy value, which is returned.

        Fails at once when a watched node dies, instead of polling it until
        the deadline; the error names the node's log.
        """
        if timeout is None:
            timeout = self._start_timeout
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            for node_id in watched_nodes:
                self._raise_if_exited(node_id)
            result = ready()
            if result:
                return result
            time.sleep(POLL_INTERVAL_SEC)

        raise TimeoutError(f"timeout waiting for {what}")

    def _raise_if_exited(self, node_id: int) -> None:
        returncode = self._procs[node_id].poll()
        if returncode is None:
            return
        log_path = self.work_dir / self._nodes[node_id].stdout_path
        raise RuntimeError(f"node{node_id} exited with {returncode}; see {log_path}")


def _validate_nodes(nodes: list[LocalMetaNode]) -> None:
    """Reject a node list that cannot produce a working cluster."""
    node_ids = [node.node_id for node in nodes]
    if not node_ids:
        raise ValueError("cluster needs at least one node")
    if len(node_ids) != len(set(node_ids)):
        raise ValueError(f"node ids must be unique, got {node_ids}")

    ports = _all_ports(nodes)
    if len(ports) != len(set(ports)):
        raise ValueError(f"listener ports must be unique, got {ports}")

    for node in nodes:
        meta_bin = node.meta_bin.expanduser()
        if not meta_bin.exists():
            raise FileNotFoundError(f"node{node.node_id} databend-meta: {meta_bin}")


def _all_ports(nodes) -> list[int]:
    return [port for node in nodes for port in node.ports.all]


def _port_in_use(port: int) -> bool:
    """Report whether something already listens on `port`."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe_socket:
        probe_socket.settimeout(PORT_PROBE_TIMEOUT_SEC)
        return probe_socket.connect_ex(("127.0.0.1", port)) == 0


def _http_get(url: str):
    """GET `url` directly, ignoring any proxy configured in the environment."""
    return _NO_PROXY_OPENER.open(url, timeout=HTTP_TIMEOUT_SEC)


def _http_ok(url: str) -> bool:
    """Report whether `url` answers with a success status."""
    try:
        with _http_get(url) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError):
        return False


def _swallow_transient(status_call, *call_args):
    """Run `status_call`, returning None while the node is not answering yet."""
    try:
        return status_call(*call_args)
    except (urllib.error.URLError, OSError, json.JSONDecodeError):
        return None


def _terminate(node_process: subprocess.Popen) -> None:
    """Stop one node process and wait for it to exit."""
    if node_process.poll() is not None:
        return

    _kill_group(node_process, signal.SIGTERM)
    try:
        node_process.wait(timeout=STOP_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        _kill_group(node_process, signal.SIGKILL)
        node_process.wait()


def _kill_group(node_process: subprocess.Popen, signal_number: int) -> None:
    """Signal a node's whole process group, tolerating a process that already exited."""
    try:
        os.killpg(node_process.pid, signal_number)
    except ProcessLookupError:
        pass


# The status response is a serialized MetaNodeStatus: `voters` is always
# present, `leader` is null until one is known, and a node's `name` is its
# raft id printed as a string.


def _extract_voters(status: dict) -> set[int]:
    """Read the voter node ids out of a cluster status response."""
    return {int(voter["name"]) for voter in status["voters"]}


def _extract_leader(status: dict) -> int | None:
    """Read the leader node id out of a cluster status response."""
    leader = status.get("leader")
    if leader is None:
        return None
    return int(leader["name"])
