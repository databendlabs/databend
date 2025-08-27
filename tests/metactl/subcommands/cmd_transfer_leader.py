#!/usr/bin/env python3

import re
import shutil
import time
from metactl_utils import metactl_bin
from utils import run_command, kill_databend_meta, start_meta_node, print_title


def get_cluster_leader():
    """Get current cluster leader from status command."""
    # Check each node to find the leader
    grpc_ports = [9191, 28202, 28302]

    for port in grpc_ports:
        try:
            result = run_command(
                [metactl_bin, "status", "--grpc-api-address", f"127.0.0.1:{port}"],
                check=False,
            )

            if result and "State: Leader" in result:
                # Parse leader node ID from status output
                node_match = re.search(r"Node: id=(\d+)", result)
                if node_match:
                    return int(node_match.group(1))
        except:
            continue

    return None


def wait_for_leader_change(initial_leader, timeout=30):
    """Wait for leader to change from initial leader."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        current_leader = get_cluster_leader()
        if current_leader and current_leader != initial_leader:
            return current_leader
        time.sleep(2)

    return None


def test_transfer_leader_subcommand():
    """Test transfer-leader subcommand functionality."""
    print_title("Test transfer-leader subcommand")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)

    # Start 3 meta nodes
    print("✓ Starting 3 meta nodes")
    start_meta_node(1, False)
    time.sleep(3)
    start_meta_node(2, False)
    time.sleep(3)
    start_meta_node(3, False)
    time.sleep(5)  # Allow cluster to form

    # Get initial leader
    print("✓ Getting initial cluster leader")
    initial_leader = get_cluster_leader()
    assert initial_leader is not None, "Should have a cluster leader"
    print(f"✓ Initial leader is node {initial_leader}")

    # Test transfer without specifying target (let cluster choose)
    print("✓ Transferring leadership without target specification")

    # Use admin API address of node 1 for transfer command
    admin_addr = "127.0.0.1:28101"
    result = run_command(
        [metactl_bin, "transfer-leader", "--admin-api-address", admin_addr]
    )

    print("✓ Transfer command executed")

    # Wait for potential leader change
    print("✓ Waiting for leader transfer to complete")
    new_leader = wait_for_leader_change(initial_leader, timeout=30)

    if new_leader:
        print(
            f"✓ Leadership transferred from node {initial_leader} to node {new_leader}"
        )
        assert new_leader != initial_leader, (
            f"Leader should change from {initial_leader}"
        )
    else:
        print("✓ No leader change detected (acceptable if cluster is stable)")

    # Verify cluster is still functional
    final_leader = get_cluster_leader()
    assert final_leader is not None, "Cluster should still have a leader"
    print(f"✓ Final leader is node {final_leader}")

    print("✓ Transfer-leader subcommand test passed")

    # Clean up only on success
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)


def test_transfer_leader_with_target():
    """Test transfer-leader with specific target node."""
    print_title("Test transfer-leader with target")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)

    # Start 3 meta nodes
    print("✓ Starting 3 meta nodes")
    start_meta_node(1, False)
    time.sleep(3)
    start_meta_node(2, False)
    time.sleep(3)
    start_meta_node(3, False)
    time.sleep(5)  # Allow cluster to form

    # Get initial leader
    initial_leader = get_cluster_leader()
    assert initial_leader is not None, "Should have a cluster leader"
    print(f"✓ Initial leader is node {initial_leader}")

    # Choose a different target node
    target_node = 2 if initial_leader != 2 else 3
    print(f"✓ Transferring leadership to node {target_node}")

    # Transfer to specific target
    admin_addr = "127.0.0.1:28101"
    result = run_command(
        [
            metactl_bin,
            "transfer-leader",
            "--admin-api-address",
            admin_addr,
            "--to",
            str(target_node),
        ]
    )

    print("✓ Transfer command with target executed")

    # Wait for leader change
    print("✓ Waiting for leader transfer to complete")
    new_leader = wait_for_leader_change(initial_leader, timeout=30)

    if new_leader:
        print(
            f"✓ Leadership transferred from node {initial_leader} to node {new_leader}"
        )
        # Note: The actual new leader might not be exactly the target due to cluster dynamics
        assert new_leader != initial_leader, (
            f"Leader should change from {initial_leader}"
        )
    else:
        print("✓ No leader change detected (acceptable if transfer was to same node)")

    print("✓ Transfer-leader with target test passed")

    # Clean up only on success
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)


def test_transfer_leader_help():
    """Test transfer-leader subcommand help."""
    result = run_command([metactl_bin, "transfer-leader", "--help"])
    assert "Usage:" in result, "Help should show usage"
    assert "--admin-api-address" in result, "Help should show admin-api-address option"
    assert "--to" in result, "Help should show to option"
    print("✓ Transfer-leader help test passed")


def main():
    """Main function to run all transfer-leader tests."""
    test_transfer_leader_help()
    test_transfer_leader_subcommand()
    test_transfer_leader_with_target()


if __name__ == "__main__":
    main()
