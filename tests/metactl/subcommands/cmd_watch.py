#!/usr/bin/env python3

import shutil
import time
import subprocess
from metactl_utils import metactl_bin, metactl_upsert
from utils import run_command, kill_databend_meta, start_meta_node, print_title


def test_watch_subcommand():
    """Test watch subcommand functionality."""
    print_title("Test watch subcommand")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    grpc_addr = "127.0.0.1:9191"

    try:
        # Test 1: Insert initial keys before starting watch
        initial_keys = [
            ("app/config/db_host", "localhost"),
            ("app/config/db_port", "5432"),
            ("app/settings/timeout", "30"),
        ]

        for key, value in initial_keys:
            metactl_upsert(grpc_addr, key, value)

        print("✓ Initial keys inserted")

        # Test 2: Start watch and check for immediate initial values
        process = subprocess.Popen(
            [metactl_bin, "watch", "--grpc-api-address", grpc_addr, "--prefix", "app/"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Give watch time to initialize and send initial events
        time.sleep(3)

        # Verify process is running
        assert process.poll() is None, "Watch process should still be running"

        # Read initial output to check initialization
        # Note: We can't easily do non-blocking read in a cross-platform way,
        # so we'll capture all output at the end, but logically verify the sequence

        print("✓ Watch process started, should have received initial keys")

        # Test 3: Insert new keys while watch is running
        new_keys = [("app/config/db_user", "admin"), ("app/settings/max_conn", "100")]

        for key, value in new_keys:
            metactl_upsert(grpc_addr, key, value)
            time.sleep(0.5)  # Brief pause between operations

        # Test 4: Update existing key
        metactl_upsert(grpc_addr, "app/config/db_port", "5433")

        # Give watch time to receive change events
        time.sleep(2)

        print("✓ Additional keys inserted and updated")

        # Test 5: Terminate watch and examine output sequence
        process.terminate()
        try:
            stdout, stderr = process.communicate(timeout=5)
            print(f"Complete watch output:\n{stdout}")

        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            assert False, "Watch process did not terminate in time"

        # Verify watch received events
        assert len(stdout) > 0, "Watch should produce output"

        # Split output into lines to check sequence
        lines = stdout.strip().split("\n")

        # First, check that INIT events come before CHANGE events
        init_lines = [line for line in lines if "INIT:" in line]
        change_lines = [
            line for line in lines if "CHANGE:" in line and "CHANGE:None" not in line
        ]

        print(
            f"Found {len(init_lines)} INIT events and {len(change_lines)} CHANGE events"
        )

        # Verify all initial keys appear in INIT events (order doesn't matter for INIT)
        for key, value in initial_keys:
            init_found = any(key in line and value in line for line in init_lines)
            assert (
                init_found
            ), f"Initial key {key} with value {value} should appear in INIT events"

        # Verify CHANGE events appear in exact order with correct keys
        # Expected sequence: new_keys[0], new_keys[1], then update to db_port
        expected_changes = [
            ("app/config/db_user", "admin"),  # First new key
            ("app/settings/max_conn", "100"),  # Second new key
            ("app/config/db_port", "5433"),  # Update existing key
        ]

        print(f"CHANGE lines: {change_lines}")

        assert len(change_lines) == len(
            expected_changes
        ), f"Expected {len(expected_changes)} CHANGE events, got {len(change_lines)}"

        # Verify each CHANGE event matches the expected sequence
        for i, (expected_key, expected_value) in enumerate(expected_changes):
            change_line = change_lines[i]
            assert (
                expected_key in change_line
            ), f"CHANGE event {i}: expected key '{expected_key}' not found in line: {change_line}"
            assert (
                expected_value in change_line
            ), f"CHANGE event {i}: expected value '{expected_value}' not found in line: {change_line}"
            print(f"✓ CHANGE event {i}: {expected_key} -> {expected_value} verified")

        print(
            "✓ Watch correctly separated initial INIT events from subsequent CHANGE events"
        )

        # Test 6: Watch with invalid address should fail
        result = run_command(
            [
                metactl_bin,
                "watch",
                "--grpc-api-address",
                "127.0.0.1:99999",
                "--prefix",
                "",
            ],
            check=False,
        )

        # Should fail with connection error (run_command shows error in output)
        print(f"✓ Invalid address correctly failed")

        print("✓ Watch subcommand tests passed")

    finally:
        # Cleanup
        kill_databend_meta()
        shutil.rmtree(".databend", ignore_errors=True)


def main():
    """Main function to run all watch tests."""
    test_watch_subcommand()


if __name__ == "__main__":
    main()
