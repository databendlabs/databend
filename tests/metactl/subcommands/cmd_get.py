#!/usr/bin/env python3

import shutil
import json
from metactl_utils import metactl_bin, verify_kv
from utils import (
    run_command, kill_databend_meta, start_meta_node,
    print_title
)


def test_get_subcommand():
    """Test get subcommand functionality."""
    print_title("Test get subcommand")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    grpc_addr = "127.0.0.1:9191"

    # Test 1: Get non-existent key should return null
    result = run_command([
        metactl_bin, "get",
        "--grpc-api-address", grpc_addr,
        "--key", "nonexistent"
    ], check=False)

    data = json.loads(result.strip())
    assert data is None, f"Expected null for nonexistent key, got {data}"

    # Test 2: Upsert and get existing key
    run_command([
        metactl_bin, "upsert",
        "--grpc-api-address", grpc_addr,
        "--key", "test_key",
        "--value", "test_value"
    ], check=False)

    result = run_command([
        metactl_bin, "get",
        "--grpc-api-address", grpc_addr,
        "--key", "test_key"
    ], check=False)

    data = json.loads(result.strip())
    assert data is not None
    assert "seq" in data and "data" in data
    actual_value = bytes(data["data"]).decode('utf-8')
    assert actual_value == "test_value"

    # Test 3: Test a few key formats using verify_kv
    test_cases = [
        ("key-dash", "value-dash"),
        ("key/slash", "value/slash"),
        ("empty_value", "")
    ]

    for key, value in test_cases:
        run_command([
            metactl_bin, "upsert",
            "--grpc-api-address", grpc_addr,
            "--key", key,
            "--value", value
        ], check=False)
        verify_kv(grpc_addr, key, value)

    print("✓ Get subcommand tests passed")


def main():
    """Main function to run all get tests."""
    try:
        test_get_subcommand()
    finally:
        kill_databend_meta()
        shutil.rmtree(".databend", ignore_errors=True)


if __name__ == "__main__":
    main()
