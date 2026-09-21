#!/usr/bin/env python3
"""Three databend-meta binaries with Raft TLS and a strict Raft secret.

The nodes join, replicate writes made through the leader and through a
follower, catch a stopped node up with a snapshot, keep writing after a leader
transfer, and take a restarted follower back. The leader's active-peer metrics
prove that peer traffic dials the TLS ports, since the plaintext Raft ports
stay open for a rolling upgrade.
"""

import json
import re
import time
from pathlib import Path

import requests
from metactl_utils import Metactl, metactl_bin, metactl_trigger_snapshot
from utils import (
    RAFT_TLS_STRICT,
    LocalMetaCluster,
    MetaNodePorts,
    build_meta_node,
    meta_cluster,
    print_step,
    print_title,
    run_command,
)

WORK_DIR = Path(".databend/metactl-raft-tls")
PORTS = {
    1: MetaNodePorts(admin=28901, grpc=28902, raft=28903, raft_tls=28904),
    2: MetaNodePorts(admin=28911, grpc=28912, raft=28913, raft_tls=28914),
    3: MetaNodePorts(admin=28921, grpc=28922, raft=28923, raft_tls=28924),
}
# Purge every applied log as soon as a snapshot holds it, so a node that
# missed those logs can only catch up by installing the snapshot.
RAFT_SETTINGS = {"max_applied_log_to_keep": 0}
START_TIMEOUT_SEC = 30
WAIT_TIMEOUT_SEC = 30
POLL_SEC = 0.5
CATCH_UP_WRITES = 20
ACTIVE_PEERS_METRIC = "metasrv_raft_network_active_peers"


def wait_for(what: str, ready, timeout: float = WAIT_TIMEOUT_SEC):
    """Poll `ready()` until it returns a truthy value, which is returned."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = ready()
        if result:
            return result
        time.sleep(POLL_SEC)
    raise TimeoutError(f"timeout waiting for {what}")


def run_ok(metactl: Metactl, args: list[str]) -> str:
    """Run one command that must succeed; return its stdout."""
    result = metactl.run(args)
    assert result.returncode == 0, f"{args} failed: {result.stderr}"
    return result.stdout


def upsert(cluster: LocalMetaCluster, via: int, key: str, value: str) -> None:
    metactl = Metactl(cluster.grpc_address(via))
    run_ok(metactl, ["upsert", "--key", key, "--value", value])


def get(cluster: LocalMetaCluster, via: int, key: str) -> str:
    metactl = Metactl(cluster.grpc_address(via))
    printed = run_ok(metactl, ["get", "--key", key])
    got = json.loads(printed)
    return bytes(got["data"]).decode()


def raft_tls_address(node_id: int) -> str:
    return f"127.0.0.1:{PORTS[node_id].raft_tls}"


def leader_of(cluster: LocalMetaCluster) -> int | None:
    leader = cluster.status().get("leader")
    if leader is None:
        return None
    return int(leader["name"])


def a_follower(cluster: LocalMetaCluster) -> int:
    leader = cluster.leader_id()
    return next(node_id for node_id in cluster.node_ids if node_id != leader)


def applied_index(cluster: LocalMetaCluster, node_id: int) -> int:
    return cluster.status(node_id)["last_applied"]["index"]


def all_applied_alike(cluster: LocalMetaCluster) -> bool:
    indexes = {applied_index(cluster, node_id) for node_id in cluster.node_ids}
    return len(indexes) == 1


def purged_beyond(cluster: LocalMetaCluster, node_id: int, index: int) -> int | None:
    """The purged log index of `node_id` once it passes `index`."""
    purged = cluster.status(node_id).get("purged")
    if purged is None or purged["index"] <= index:
        return None
    return purged["index"]


def active_peers(cluster: LocalMetaCluster, node_id: int) -> dict[int, set[str]]:
    """Peer id -> the addresses `node_id` currently dials it at."""
    url = f"http://{cluster.admin_address(node_id)}/v1/metrics"
    text = requests.get(url, timeout=5).text
    pattern = rf"{ACTIVE_PEERS_METRIC}\{{([^}}]*)\}} (\S+)"
    peers: dict[int, set[str]] = {}
    for labels, value in re.findall(pattern, text):
        if float(value) <= 0:
            continue
        label = dict(re.findall(r'(\w+)="([^"]*)"', labels))
        peers.setdefault(int(label["id"]), set()).add(label["addr"])
    return peers


def check_membership(cluster: LocalMetaCluster) -> None:
    print_step(
        "nodes 2 and 3 joined node 1; every voter publishes its Raft TLS address"
    )
    voters = {int(voter["name"]): voter for voter in cluster.status()["voters"]}
    assert sorted(voters) == [1, 2, 3], sorted(voters)
    for node_id, voter in voters.items():
        published = voter.get("raft_tls_advertise_address")
        assert published == raft_tls_address(node_id), voter


def check_peer_traffic_uses_tls(cluster: LocalMetaCluster) -> None:
    print_step("the leader dials every peer at its TLS port")
    leader = cluster.leader_id()
    followers = [node_id for node_id in cluster.node_ids if node_id != leader]

    def every_follower_dialed():
        peers = active_peers(cluster, leader)
        if all(follower in peers for follower in followers):
            return peers
        return None

    peers = wait_for("active peers on the leader", every_follower_dialed)
    for follower in followers:
        assert peers[follower] == {raft_tls_address(follower)}, peers


def check_replication(cluster: LocalMetaCluster) -> None:
    leader = cluster.leader_id()
    follower = a_follower(cluster)
    for label, via in (("leader", leader), ("follower", follower)):
        print_step(f"a write through the {label} is readable through every node")
        key = f"raft-tls/via-{label}"
        upsert(cluster, via, key, label)
        for node_id in cluster.node_ids:
            value = get(cluster, node_id, key)
            assert value == label, (node_id, value)
    wait_for("every node to apply the same log", lambda: all_applied_alike(cluster))


def check_snapshot_catch_up(cluster: LocalMetaCluster) -> None:
    print_step("a stopped node catches up through a snapshot")
    leader = cluster.leader_id()
    stopped = a_follower(cluster)
    last_log_before = cluster.status(stopped)["last_log_index"]
    cluster.stop_node(stopped)

    for i in range(CATCH_UP_WRITES):
        upsert(cluster, leader, f"raft-tls/catch-up/{i}", str(i))
    metactl_trigger_snapshot(cluster.admin_address(leader))
    purged = wait_for(
        "the leader to purge the logs the stopped node missed",
        lambda: purged_beyond(cluster, leader, last_log_before),
    )

    print(
        f"node{stopped} last log before stop: {last_log_before}; leader purged: {purged}"
    )

    cluster.start_node(stopped)
    wait_for(
        f"node {stopped} to apply the purged logs",
        lambda: applied_index(cluster, stopped) >= purged,
    )
    print(f"node{stopped} applied after restart: {applied_index(cluster, stopped)}")
    last = CATCH_UP_WRITES - 1
    value = get(cluster, stopped, f"raft-tls/catch-up/{last}")
    assert value == str(last), value


def check_leader_transfer(cluster: LocalMetaCluster) -> None:
    print_step("writes continue after a leader transfer")
    old_leader = cluster.leader_id()
    target = a_follower(cluster)
    run_command(
        [
            metactl_bin,
            "transfer-leader",
            "--to",
            str(target),
            "--admin-api-address",
            cluster.admin_address(old_leader),
        ]
    )
    wait_for(f"node {target} to lead", lambda: leader_of(cluster) == target)
    upsert(cluster, target, "raft-tls/after-transfer", "ok")
    for node_id in cluster.node_ids:
        value = get(cluster, node_id, "raft-tls/after-transfer")
        assert value == "ok", (node_id, value)


def check_restart_rejoins(cluster: LocalMetaCluster) -> None:
    print_step("a restarted follower rejoins and serves reads")
    follower = a_follower(cluster)
    cluster.restart_node(follower)
    wait_for("every node to apply the same log", lambda: all_applied_alike(cluster))
    value = get(cluster, follower, "raft-tls/after-transfer")
    assert value == "ok", value


def main():
    print_title("Test three databend-meta nodes with Raft TLS and a strict Raft secret")
    join = (f"127.0.0.1:{PORTS[1].raft}",)
    nodes = []
    for node_id in (1, 2, 3):
        node = build_meta_node(
            node_id,
            PORTS[node_id],
            security=RAFT_TLS_STRICT,
            join_addresses=() if node_id == 1 else join,
            raft_settings=RAFT_SETTINGS,
        )
        nodes.append(node)
    with meta_cluster(WORK_DIR, nodes, start_timeout=START_TIMEOUT_SEC) as cluster:
        check_membership(cluster)
        check_peer_traffic_uses_tls(cluster)
        check_replication(cluster)
        check_snapshot_catch_up(cluster)
        check_leader_transfer(cluster)
        check_restart_rejoins(cluster)
        check_peer_traffic_uses_tls(cluster)
    print("✓ Raft TLS cluster tests passed")


if __name__ == "__main__":
    main()
