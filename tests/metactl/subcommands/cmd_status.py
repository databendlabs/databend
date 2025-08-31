#!/usr/bin/env python3

import re
import shutil
from metactl_utils import metactl_bin, cluster_status
from utils import run_command, kill_databend_meta, start_meta_node, print_title


def verify_status_format(result):
    """Simple verification of status output format."""
    lines = result.strip().split("\n")

    # Expected fields that should appear in status output
    expected_fields = [
        "BinaryVersion:",
        "DataVersion:",
        "RaftLogSize:",
        "SnapshotKeyCount:",
        "Node:",
        "State:",
        "CurrentTerm:",
        "LastSeq:",
        "LastLogIndex:",
        "LastApplied:",
    ]

    # Check each expected field exists
    status_text = result
    found_fields = []
    for field in expected_fields:
        if field in status_text:
            found_fields.append(field)
            # Extract value after colon for basic validation
            for line in lines:
                if line.startswith(field):
                    value = line.split(":", 1)[1].strip()
                    print(f"✓ {field} {value}")
                    break

    missing_fields = [f for f in expected_fields if f not in found_fields]
    assert not missing_fields, f"Missing expected fields: {missing_fields}"

    # Check for some specific format patterns
    assert (
        "State: Leader" in result
        or "State: Follower" in result
        or "State: Candidate" in result
    ), "State should be Leader, Follower, or Candidate"

    # Check if Node info exists with expected format
    node_pattern = r"Node: id=\d+ raft=.+:\d+"
    assert re.search(
        node_pattern, result
    ), "Node format should match 'id=X raft=host:port'"

    # Check LastApplied format
    last_applied_pattern = r"LastApplied: T\d+-N\d+\.\d+"
    assert re.search(
        last_applied_pattern, result
    ), "LastApplied should match 'TX-NX.X' format"

    print(
        f"✓ Status format verification passed: {len(found_fields)}/{len(expected_fields)} fields found"
    )


def test_status_subcommand():
    """Test status subcommand functionality."""
    print_title("Test status subcommand")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    grpc_addr = "127.0.0.1:9191"

    # Test status command
    result = run_command([metactl_bin, "status", "--grpc-api-address", grpc_addr])

    # Verify status output contains data
    assert result, "Status should return data"
    assert len(result.strip()) > 0, "Status should produce output"

    print("Raw status output:")
    print(result)

    # Simple format verification
    verify_status_format(result)

    print("✓ Status subcommand test passed")

    # Clean up only on success
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)


def test_status_help():
    """Test status subcommand help."""
    result = run_command([metactl_bin, "status", "--help"])
    assert "Usage:" in result, "Help should show usage"
    assert "--grpc-api-address" in result, "Help should show grpc-api-address option"
    print("✓ Status help test passed")


def main():
    """Main function to run all status tests."""
    test_status_help()
    test_status_subcommand()


if __name__ == "__main__":
    main()
