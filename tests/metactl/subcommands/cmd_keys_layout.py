#!/usr/bin/env python3

import shutil
import time
from metactl_utils import metactl_bin, metactl_upsert, metactl_trigger_snapshot
from utils import run_command, kill_databend_meta, start_meta_node, print_title


def test_keys_layout_from_grpc():
    """Test keys-layout subcommand from grpc address."""
    print_title("Test keys-layout from GRPC")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    grpc_addr = "127.0.0.1:9191"
    admin_addr = "127.0.0.1:28101"

    # Insert hierarchical test data
    test_keys = [
        ("databases/1/tables/users/columns/id", "id_data"),
        ("databases/1/tables/users/columns/name", "name_data"),
        ("databases/1/tables/users/columns/email", "email_data"),
        ("databases/1/tables/orders/columns/id", "order_id_data"),
        ("databases/1/tables/orders/columns/user_id", "user_id_data"),
        ("databases/2/tables/products/columns/id", "product_id_data"),
        ("databases/2/tables/products/columns/name", "product_name_data"),
        ("system/config/max_connections", "100"),
        ("system/stats/query_count", "42"),
    ]

    for key, value in test_keys:
        metactl_upsert(grpc_addr, key, value)
    print("✓ Hierarchical test data inserted")

    # Trigger snapshot to ensure keys-layout has data
    metactl_trigger_snapshot(admin_addr)
    print("✓ Snapshot triggered")

    # Wait for snapshot to complete
    time.sleep(2)

    # Test keys-layout with no depth limit (all levels)
    print("\n--- Testing keys-layout with no depth limit ---")
    result = run_command([metactl_bin, "keys-layout", "--grpc-api-address", grpc_addr])

    data_lines = result.strip().split("\n")

    for line in data_lines:
        print("Got:", line)

    # Expected output: hierarchical prefixes with their counts
    expected_prefixes = [
        "databases/1/tables/orders/columns 2",
        "databases/1/tables/orders 2",
        "databases/1/tables/users/columns 3",
        "databases/1/tables/users 3",
        "databases/1/tables 5",
        "databases/1 5",
        "databases/2/tables/products/columns 2",
        "databases/2/tables/products 2",
        "databases/2/tables 2",
        "databases/2 2",
        "databases 7",
        "system/config 1",
        "system/stats 1",
        "system 2",
        " 9",  # Total count with empty prefix
    ]

    # Verify keys-layout result contains expected data
    assert result, "Keys-layout from GRPC should return data"
    assert len(data_lines) > 0, "Keys-layout should produce output lines"

    print(f"Got {len(data_lines)} data lines, expected {len(expected_prefixes)} lines")
    assert len(data_lines) == len(expected_prefixes), (
        f"Line count mismatch: got {len(data_lines)}, expected {len(expected_prefixes)}"
    )

    # Compare output line by line
    for i, (actual_line, expected_line) in enumerate(zip(data_lines, expected_prefixes)):
        assert actual_line == expected_line, (
            f"Line {i} mismatch:\nActual: '{actual_line}'\nExpected: '{expected_line}'"
        )

    print(f"✓ All {len(data_lines)} prefix count lines match expected output")

    # Test keys-layout with depth=1 (only top-level prefixes)
    print("\n--- Testing keys-layout with depth=1 ---")
    result_depth1 = run_command([metactl_bin, "keys-layout", "--grpc-api-address", grpc_addr, "--depth", "1"])

    depth1_data = result_depth1.strip().split("\n")

    for line in depth1_data:
        print("Got:", line)

    expected_depth1 = [
        "databases 7",
        "system 2",
        " 9",  # Total
    ]

    assert len(depth1_data) == len(expected_depth1), (
        f"Depth=1 line count mismatch: got {len(depth1_data)}, expected {len(expected_depth1)}"
    )

    for i, (actual_line, expected_line) in enumerate(zip(depth1_data, expected_depth1)):
        assert actual_line == expected_line, (
            f"Depth=1 line {i} mismatch:\nActual: '{actual_line}'\nExpected: '{expected_line}'"
        )

    print(f"✓ Depth=1 test passed: {len(depth1_data)} lines match expected output")

    # Test keys-layout with depth=2 (up to 1 slash deep)
    print("\n--- Testing keys-layout with depth=2 ---")
    result_depth2 = run_command([metactl_bin, "keys-layout", "--grpc-api-address", grpc_addr, "--depth", "2"])

    depth2_data = result_depth2.strip().split("\n")

    for line in depth2_data:
        print("Got:", line)

    expected_depth2 = [
        "databases/1 5",
        "databases/2 2",
        "databases 7",
        "system/config 1",
        "system/stats 1",
        "system 2",
        " 9",  # Total
    ]

    assert len(depth2_data) == len(expected_depth2), (
        f"Depth=2 line count mismatch: got {len(depth2_data)}, expected {len(expected_depth2)}"
    )

    for i, (actual_line, expected_line) in enumerate(zip(depth2_data, expected_depth2)):
        assert actual_line == expected_line, (
            f"Depth=2 line {i} mismatch:\nActual: '{actual_line}'\nExpected: '{expected_line}'"
        )

    print(f"✓ Depth=2 test passed: {len(depth2_data)} lines match expected output")

    # Clean up only on success
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)


def main():
    """Main function to run all keys-layout tests."""
    test_keys_layout_from_grpc()


if __name__ == "__main__":
    main()
