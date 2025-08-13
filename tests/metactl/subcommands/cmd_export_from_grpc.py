#!/usr/bin/env python3

import json
import shutil
import time
from metactl_utils import metactl_bin, metactl_upsert, metactl_trigger_snapshot
from utils import run_command, kill_databend_meta, start_meta_node, print_title


def test_export_from_grpc():
    """Test export subcommand from grpc address."""
    print_title("Test export from GRPC")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    grpc_addr = "127.0.0.1:9191"
    admin_addr = "127.0.0.1:28101"

    # Insert test data before snapshot
    test_keys = [
        ("app/db/host", "localhost"),
        ("app/db/port", "5432"),
        ("app/config/timeout", "30"),
    ]

    for key, value in test_keys:
        metactl_upsert(grpc_addr, key, value)
    print("✓ Test data inserted")

    # Trigger snapshot to ensure export has data
    metactl_trigger_snapshot(admin_addr)
    print("✓ Snapshot triggered")

    # Wait for snapshot to complete
    time.sleep(2)

    # Test export from grpc
    result = run_command([metactl_bin, "export", "--grpc-api-address", grpc_addr])

    lines = result.strip().split("\n")
    for l in lines:
        print("Got:", l)

    want = """["header",{"DataHeader":{"key":"header","value":{"version":"V004"}}}]
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
["state_machine/0",{"GenericKV":{"key":"app/db/port","value":{"seq":2,"meta":null,"data":[53,52,51,50]}}}]""".strip().split(
        "\n"
    )

    # Verify export result contains data
    assert result, "Export from GRPC should return data"
    assert len(lines) > 0, "Export should produce output lines"

    # Compare with expected output by converting all to JSON
    print(f"Got {len(lines)} lines, expected {len(want)} lines")
    assert len(lines) == len(want), (
        f"Line count mismatch: got {len(lines)}, expected {len(want)}"
    )

    def normalize_json(obj):
        """Remove dynamic fields like time_ms from JSON object for comparison"""
        try:
            # Directly try to remove time_ms if it exists
            del obj[1]["LogEntry"]["payload"]["Normal"]["time_ms"]
        except:
            pass
        return obj

    # Compare JSON objects line by line, parsing on the fly
    for i, (actual_line, want_line) in enumerate(zip(lines, want)):
        try:
            actual_json = json.loads(actual_line)
        except json.JSONDecodeError as e:
            assert False, f"Invalid JSON at line {i}: {actual_line}, error: {e}"

        actual_json = normalize_json(actual_json)

        try:
            want_json = json.loads(want_line)
        except json.JSONDecodeError as e:
            assert False, f"Invalid JSON in expected line {i}: {want_line}, error: {e}"

        want_json = normalize_json(want_json)

        assert actual_json == want_json, (
            f"Line {i} JSON mismatch:\nActual: {actual_json}\nExpected: {want_json}"
        )

    print(f"✓ All {len(lines)} JSON lines match expected output")

    # Clean up only on success
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)


def main():
    """Main function to run all export-from-grpc tests."""
    test_export_from_grpc()


if __name__ == "__main__":
    main()
