#!/usr/bin/env python3

import shutil
import json
from metactl_utils import metactl_bin, verify_kv
from utils import (
    run_command, kill_databend_meta, start_meta_node,
    print_title, BUILD_PROFILE
)


def test_upsert_and_verify():
    """A concise test for upserting and verifying multiple key-value pairs."""
    print_title("Test upsert and verify")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    grpc_addr = "127.0.0.1:9191"

    test_cases = [
        ("key1", "value1"),
        ("key2", json.dumps({"a": 1, "b": [2, 3]})),
        ("key-with-dash", "value-with-dash"),
        ("key/with/slash", "value/with/slash"),
        ("key_with_underscore", "value_with_underscore"),
        ("overwrite_key", "initial_value"),
    ]

    for key, value in test_cases:
        run_command([
            metactl_bin, "upsert",
            "--grpc-api-address", grpc_addr,
            "--key", key,
            "--value", value
        ], check=False)
        verify_kv(grpc_addr, key, value)

    # Overwrite a key and verify the new value
    run_command([
        metactl_bin, "upsert",
        "--grpc-api-address", grpc_addr,
        "--key", "overwrite_key",
        "--value", "new_value"
    ], check=False)
    verify_kv(grpc_addr, "overwrite_key", "new_value")

    # Test empty value
    run_command([
        metactl_bin, "upsert",
        "--grpc-api-address", grpc_addr,
        "--key", "empty_key",
        "--value", ""
    ], check=False)
    verify_kv(grpc_addr, "empty_key", "")

    print("✓ All upsert and verify tests passed")

    # Cleanup only when success.
    # Otherwise, leave the state for checking
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)



def main():
    """Main function to run all upsert tests."""
    test_upsert_and_verify()


if __name__ == "__main__":
    main()
