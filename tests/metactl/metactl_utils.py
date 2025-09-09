#!/usr/bin/env python3

import sys
import json
import subprocess
import time
from typing import Dict
import requests
from pathlib import Path
from utils import print_step, BUILD_PROFILE, run_command

metactl_bin = f"./target/{BUILD_PROFILE}/databend-metactl"


def metactl_run_lua(lua_script=None, lua_filename=None):
    cmds = [metactl_bin, "lua"]

    if lua_filename:
        cmds += [lua_filename]
        print("metactl_run_lua from_file:", lua_filename, file=sys.stderr)
    else:
        print("metactl_run_lua from_stdin:", lua_script, file=sys.stderr)

    result = subprocess.run(cmds, input=lua_script, capture_output=True, text=True)

    print("metactl_run_lua result:", result, file=sys.stderr)

    if result.returncode != 0:
        raise Exception(result)

    output = result.stdout.strip()
    return output


def metactl_upsert(grpc_addr, key, value):
    """Upsert a key-value pair using the upsert subcommand."""
    result = run_command(
        [
            metactl_bin,
            "upsert",
            "--grpc-api-address",
            grpc_addr,
            "--key",
            key,
            "--value",
            value,
        ]
    )
    return result


def metactl_trigger_snapshot(admin_addr):
    """Trigger snapshot creation using the trigger-snapshot subcommand."""
    result = run_command(
        [metactl_bin, "trigger-snapshot", "--admin-api-address", admin_addr]
    )
    return result


def verify_kv(grpc_addr, key, expected_value=None):
    """Verify a key-value pair using the get subcommand."""
    time.sleep(0.5)  # Brief sleep to ensure data is persisted

    result = run_command(
        [metactl_bin, "get", "--grpc-api-address", grpc_addr, "--key", key], check=False
    )

    print(f"Get result for key '{key}': {result}")

    if not result.strip():
        assert False, f"Key '{key}' not found or empty result"

    try:
        data = json.loads(result.strip())
    except json.JSONDecodeError as e:
        assert False, f"Failed to parse JSON result: {e}, result: {result}"

    print(f"Parsed data: {data}")

    if data is None:
        assert expected_value is None, f"Expected None but got {expected_value}"
    else:
        # Extract the actual value from the stored data
        actual_value = bytes(data["data"]).decode("utf-8")
        print(f"Actual value: '{actual_value}', Expected: '{expected_value}'")

        if expected_value is not None:
            assert (
                actual_value == expected_value
            ), f"Expected '{expected_value}', got '{actual_value}'"


def metactl_export_from_grpc(addr: str) -> str:
    """Export meta data from grpc-address to stdout"""
    print_step(f"Start: Export meta data from {addr} to stdout")

    cmd = [metactl_bin, "export", "--grpc-api-address", addr]

    result = run_command(cmd, check=True)

    print_step(f"Done: Exported meta data to stdout")
    return result


def metactl_export(meta_dir: str, output_path: str) -> str:
    """Export meta data from raft directory to database file or stdout"""
    print_step(f"Start: Export meta data from {meta_dir} to {output_path}")

    cmd = [metactl_bin, "export", "--raft-dir", meta_dir]

    if output_path:
        cmd.append("--db")
        cmd.append(output_path)

    result = run_command(cmd, check=False)

    print_step(f"Done: Exported meta data to {output_path}")
    return result


def metactl_import(
    meta_dir: str, id: int, db_path: str, initial_cluster: Dict[int, str]
):
    """Import meta data from database file to raft directory"""
    cmd = [
        metactl_bin,
        "import",
        "--raft-dir",
        meta_dir,
        "--id",
        str(id),
        "--db",
        db_path,
    ]

    for id, addr in initial_cluster.items():
        cmd.append("--initial-cluster")
        cmd.append(f"{id}={addr}")

    result = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result.wait()
    print(result.stdout.read().decode())
    print(result.stderr.read().decode())


def cluster_status(msg: str) -> str:
    """Get cluster status from meta service endpoint"""
    print_step(f"Check /v1/cluster/status {msg} start")

    response = requests.get("http://127.0.0.1:28101/v1/cluster/status")
    status = response.json()

    print_step(f"Check /v1/cluster/status {msg} end")

    return status
