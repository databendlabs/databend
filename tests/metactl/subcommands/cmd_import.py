#!/usr/bin/env python3

import json
import shutil
import os
import time
import subprocess
from metactl_utils import metactl_bin, metactl_export_from_grpc
from utils import run_command, kill_databend_meta, start_meta_node, print_title


def test_import_subcommand():
    """Test import subcommand functionality."""
    print_title("Test import subcommand")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)

    target_raft_dir = "./.databend/meta1"
    grpc_addr = "127.0.0.1:9191"

    # Test data from export_from_grpc test
    test_data = """["header",{"DataHeader":{"key":"header","value":{"version":"V004"}}}]
["raft_log",{"NodeId":1}]
["raft_log",{"Vote":{"leader_id":{"term":1,"node_id":1},"committed":true}}]
["raft_log",{"Committed":{"leader_id":{"term":1,"node_id":1},"index":7}}]
["raft_log",{"Purged":null}]
["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":0,"node_id":0},"index":0},"payload":{"Membership":{"configs":[[1]],"nodes":{"1":{}}}}}}]
["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":1},"payload":"Blank"}}]
["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":2},"payload":{"Normal":{"time_ms":1753508277701,"cmd":{"AddNode":{"node_id":1,"node":{"name":"1","endpoint":{"addr":"localhost","port":28103},"grpc_api_advertise_address":"127.0.0.1:9191"},"overriding":false}}}}}}]
["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":3},"payload":{"Membership":{"configs":[[1]],"nodes":{"1":{}}}}}}]
["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":4},"payload":{"Normal":{"time_ms":1753508277725,"cmd":{"AddNode":{"node_id":1,"node":{"name":"1","endpoint":{"addr":"localhost","port":28103},"grpc_api_advertise_address":"127.0.0.1:9191"},"overriding":true}}}}}}]
["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":5},"payload":{"Normal":{"time_ms":1753508279640,"cmd":{"UpsertKV":{"key":"app/db/host","seq":{"GE":0},"value":{"Update":[108,111,99,97,108,104,111,115,116]},"value_meta":null}}}}}}]
["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":6},"payload":{"Normal":{"time_ms":1753508279673,"cmd":{"UpsertKV":{"key":"app/db/port","seq":{"GE":0},"value":{"Update":[53,52,51,50]},"value_meta":null}}}}}}]
["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":7},"payload":{"Normal":{"time_ms":1753508279702,"cmd":{"UpsertKV":{"key":"app/config/timeout","seq":{"GE":0},"value":{"Update":[51,48]},"value_meta":null}}}}}}]
["state_machine/0",{"Sequences":{"key":"generic-kv","value":3}}]
["state_machine/0",{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"leader_id":{"term":1,"node_id":1},"index":7}}}}]
["state_machine/0",{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":3},"membership":{"configs":[[1]],"nodes":{"1":{}}}}}}}]
["state_machine/0",{"Nodes":{"key":1,"value":{"name":"1","endpoint":{"addr":"localhost","port":28103},"grpc_api_advertise_address":"127.0.0.1:9191"}}}]
["state_machine/0",{"GenericKV":{"key":"app/config/timeout","value":{"seq":3,"meta":null,"data":[51,48]}}}]
["state_machine/0",{"GenericKV":{"key":"app/db/host","value":{"seq":1,"meta":null,"data":[108,111,99,97,108,104,111,115,116]}}}]
["state_machine/0",{"GenericKV":{"key":"app/db/port","value":{"seq":2,"meta":null,"data":[53,52,51,50]}}}]"""

    print("✓ Using embedded test data for import")

    # Import data via stdin while meta service is stopped
    print("✓ Importing data to raft directory via stdin")
    process = subprocess.Popen(
        [
            metactl_bin,
            "import",
            "--raft-dir",
            target_raft_dir,
            "--id",
            "1",
            "--initial-cluster",
            "1=localhost:28103",
        ],
        stdin=subprocess.PIPE,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = process.communicate(input=test_data)
    if process.returncode != 0:
        print(f"Import failed: {stderr}")
        assert False, f"Import command failed with return code {process.returncode}"

    # Verify raft directory was created
    assert os.path.exists(
        target_raft_dir
    ), f"Raft directory should exist: {target_raft_dir}"
    print(f"✓ Raft directory created: {target_raft_dir}")

    # Check for raft log files in correct location
    log_dir = os.path.join(target_raft_dir, "df_meta", "V004", "log")
    if os.path.exists(log_dir):
        log_files = [f for f in os.listdir(log_dir) if f.endswith(".wal")]
        assert len(log_files) > 0, "Should have raft log files"
        print(f"✓ Found {len(log_files)} raft log files in {log_dir}")
    else:
        assert False, f"Log directory should exist: {log_dir}"

    # Check for snapshot files
    snapshot_dir = os.path.join(target_raft_dir, "df_meta", "V004", "snapshot")
    if os.path.exists(snapshot_dir):
        snapshot_files = [f for f in os.listdir(snapshot_dir) if f.endswith(".snap")]
        print(f"✓ Snapshot directory contains {len(snapshot_files)} snapshot files")
    else:
        print("✓ No snapshot directory (expected for fresh import)")

    # Start meta service after import
    print("✓ Starting meta service after import")
    start_meta_node(1, False)
    time.sleep(2)  # Allow service to initialize

    # Export data via GRPC and verify content
    print("✓ Exporting data via GRPC to verify import")
    exported_data = metactl_export_from_grpc(grpc_addr)

    # Verify export contains data
    assert exported_data, "Export should return data"
    assert len(exported_data.strip()) > 0, "Export should produce output"

    lines = exported_data.strip().split("\n")
    print(f"✓ Exported {len(lines)} lines from imported data")

    # Verify header and basic structure
    assert lines[0].startswith('["header"'), "First line should be header"

    # Parse first line to check version
    header_data = json.loads(lines[0])
    assert (
        header_data[1]["DataHeader"]["value"]["version"] == "V004"
    ), "Should import V004 data"
    print("✓ Imported data version verification passed")

    # Check for required sections
    has_raft_log = any('"raft_log"' in line for line in lines)
    has_state_machine = any('"state_machine"' in line for line in lines)

    assert has_raft_log, "Export should contain raft_log entries"
    print("✓ Exported data contains raft log entries")

    if has_state_machine:
        print("✓ Exported data contains state machine entries")

    print("✓ Import subcommand test passed")

    # Clean up only on success
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)


def test_import_help():
    """Test import subcommand help."""
    result = run_command([metactl_bin, "import", "--help"])
    assert "Usage:" in result, "Help should show usage"
    assert "--raft-dir" in result, "Help should show raft-dir option"
    assert "--initial-cluster" in result, "Help should show initial-cluster option"
    print("✓ Import help test passed")


def main():
    """Main function to run all import tests."""
    test_import_help()
    test_import_subcommand()


if __name__ == "__main__":
    main()
