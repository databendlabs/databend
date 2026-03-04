#!/usr/bin/env python3

import os
import re
import shutil
import time
from metactl_utils import metactl_bin, metactl_upsert, metactl_trigger_snapshot
from utils import run_command, kill_databend_meta, start_meta_node, print_title


def mask_time(text):
    """Mask time fields in the output for comparison."""
    text = re.sub(
        r"time: \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+", "time: <TIME>", text
    )
    return text


def compare_output(actual_lines, expected_str):
    """Compare actual output against expected. Returns (success, error_msg)."""
    expected_lines = expected_str.strip().split("\n")
    if len(actual_lines) != len(expected_lines):
        return False, f"Line count mismatch: got {len(actual_lines)}, expected {len(expected_lines)}"
    for i, (actual_line, expected_line) in enumerate(zip(actual_lines, expected_lines)):
        if actual_line != expected_line:
            return False, f"Line {i} mismatch:\n  Actual  : {actual_line}\n  Expected: {expected_line}"
    return True, None


def assert_match_any(result, expected_outputs):
    """Assert that result matches one of the expected outputs (time-masked)."""
    actual_lines = result.strip().split("\n")
    actual_masked = [mask_time(line) for line in actual_lines]

    for i, exp in enumerate(expected_outputs):
        success, error_msg = compare_output(actual_masked, exp)
        if success:
            print(f"✓ All {len(actual_masked)} lines match expected output #{i+1} (time fields masked)")
            return
    # None matched, show error from first expected
    _, error_msg = compare_output(actual_masked, expected_outputs[0])
    assert False, error_msg


def setup_wal_data():
    """Start meta node, insert test data, stop node. Returns WAL directory."""
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    grpc_addr = "127.0.0.1:9191"
    admin_addr = "127.0.0.1:28101"

    test_keys = [
        ("app/db/host", "localhost"),
        ("app/db/port", "5432"),
        ("app/config/timeout", "30"),
    ]

    for key, value in test_keys:
        metactl_upsert(grpc_addr, key, value)
    print("✓ Test data inserted")

    metactl_trigger_snapshot(admin_addr)
    print("✓ Snapshot triggered")

    time.sleep(2)
    kill_databend_meta()


def test_dump_raft_log_wal():
    """Test dump-raft-log-wal subcommand from raft directory."""
    print_title("Test dump-raft-log-wal from raft directory")
    setup_wal_data()

    result = run_command(
        [metactl_bin, "dump-raft-log-wal", "--raft-dir", "./.databend/meta1"]
    )

    print("Output:")
    print(result)

    # Expected output with time field that will be masked
    expected = """RaftLog:
ChunkId(00_000_000_000_000_000_000)
  R-00000: [000_000_000, 000_000_018) Size(18): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: None))
  R-00001: [000_000_018, 000_000_046) Size(28): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: LogStoreMeta{ node_id: Some(1) }))
  R-00002: [000_000_046, 000_000_125) Size(79): Append(log_id: T0-N1.0, payload: membership:{voters:[{1:EmptyNode}], learners:[]})
  R-00003: [000_000_125, 000_000_175) Size(50): SaveVote(<T1-N1:->)
  R-00004: [000_000_175, 000_000_225) Size(50): SaveVote(<T1-N1:Q>)
  R-00005: [000_000_225, 000_000_277) Size(52): Append(log_id: T1-N1.1, payload: blank)
  R-00006: [000_000_277, 000_000_472) Size(195): Append(log_id: T1-N1.2, payload: normal:time: <TIME> cmd: add_node(no-override):1=id=1 raft=localhost:28103 grpc=127.0.0.1:9191)
  R-00007: [000_000_472, 000_000_518) Size(46): Commit(T1-N1.1)
  R-00008: [000_000_518, 000_000_564) Size(46): Commit(T1-N1.2)
  R-00009: [000_000_564, 000_000_643) Size(79): Append(log_id: T1-N1.3, payload: membership:{voters:[{1:EmptyNode}], learners:[]})
  R-00010: [000_000_643, 000_000_689) Size(46): Commit(T1-N1.3)
  R-00011: [000_000_689, 000_000_884) Size(195): Append(log_id: T1-N1.4, payload: normal:time: <TIME> cmd: add_node(override):1=id=1 raft=localhost:28103 grpc=127.0.0.1:9191)
  R-00012: [000_000_884, 000_000_930) Size(46): Commit(T1-N1.4)
  R-00013: [000_000_930, 000_001_192) Size(262): Append(log_id: T1-N1.5, payload: normal:time: <TIME> cmd: txn:TxnRequest{if:[app/db/host >= seq(0)] then:[Put(Put key=app/db/host)] else:[Get(Get key=app/db/host)]})
  R-00014: [000_001_192, 000_001_238) Size(46): Commit(T1-N1.5)
  R-00015: [000_001_238, 000_001_495) Size(257): Append(log_id: T1-N1.6, payload: normal:time: <TIME> cmd: txn:TxnRequest{if:[app/db/port >= seq(0)] then:[Put(Put key=app/db/port)] else:[Get(Get key=app/db/port)]})
  R-00016: [000_001_495, 000_001_541) Size(46): Commit(T1-N1.6)
  R-00017: [000_001_541, 000_001_817) Size(276): Append(log_id: T1-N1.7, payload: normal:time: <TIME> cmd: txn:TxnRequest{if:[app/config/timeout >= seq(0)] then:[Put(Put key=app/config/timeout)] else:[Get(Get key=app/config/timeout)]})
  R-00018: [000_001_817, 000_001_863) Size(46): Commit(T1-N1.7)"""

    expected2 = """RaftLog:
ChunkId(00_000_000_000_000_000_000)
  R-00000: [000_000_000, 000_000_018) Size(18): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: None))
  R-00001: [000_000_018, 000_000_046) Size(28): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: LogStoreMeta{ node_id: Some(1) }))
  R-00002: [000_000_046, 000_000_125) Size(79): Append(log_id: T0-N1.0, payload: membership:{voters:[{1:EmptyNode}], learners:[]})
  R-00003: [000_000_125, 000_000_175) Size(50): SaveVote(<T1-N1:->)
  R-00004: [000_000_175, 000_000_225) Size(50): SaveVote(<T1-N1:Q>)
  R-00005: [000_000_225, 000_000_277) Size(52): Append(log_id: T1-N1.1, payload: blank)
  R-00006: [000_000_277, 000_000_323) Size(46): Commit(T1-N1.1)
  R-00007: [000_000_323, 000_000_518) Size(195): Append(log_id: T1-N1.2, payload: normal:time: <TIME> cmd: add_node(no-override):1=id=1 raft=localhost:28103 grpc=127.0.0.1:9191)
  R-00008: [000_000_518, 000_000_564) Size(46): Commit(T1-N1.2)
  R-00009: [000_000_564, 000_000_643) Size(79): Append(log_id: T1-N1.3, payload: membership:{voters:[{1:EmptyNode}], learners:[]})
  R-00010: [000_000_643, 000_000_689) Size(46): Commit(T1-N1.3)
  R-00011: [000_000_689, 000_000_884) Size(195): Append(log_id: T1-N1.4, payload: normal:time: <TIME> cmd: add_node(override):1=id=1 raft=localhost:28103 grpc=127.0.0.1:9191)
  R-00012: [000_000_884, 000_000_930) Size(46): Commit(T1-N1.4)
  R-00013: [000_000_930, 000_001_192) Size(262): Append(log_id: T1-N1.5, payload: normal:time: <TIME> cmd: txn:TxnRequest{if:[app/db/host >= seq(0)] then:[Put(Put key=app/db/host)] else:[Get(Get key=app/db/host)]})
  R-00014: [000_001_192, 000_001_238) Size(46): Commit(T1-N1.5)
  R-00015: [000_001_238, 000_001_495) Size(257): Append(log_id: T1-N1.6, payload: normal:time: <TIME> cmd: txn:TxnRequest{if:[app/db/port >= seq(0)] then:[Put(Put key=app/db/port)] else:[Get(Get key=app/db/port)]})
  R-00016: [000_001_495, 000_001_541) Size(46): Commit(T1-N1.6)
  R-00017: [000_001_541, 000_001_817) Size(276): Append(log_id: T1-N1.7, payload: normal:time: <TIME> cmd: txn:TxnRequest{if:[app/config/timeout >= seq(0)] then:[Put(Put key=app/config/timeout)] else:[Get(Get key=app/config/timeout)]})
  R-00018: [000_001_817, 000_001_863) Size(46): Commit(T1-N1.7)"""

    expected3 = """RaftLog:
ChunkId(00_000_000_000_000_000_000)
  R-00000: [000_000_000, 000_000_018) Size(18): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: None))
  R-00001: [000_000_018, 000_000_046) Size(28): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: LogStoreMeta{ node_id: Some(1) }))
  R-00002: [000_000_046, 000_000_125) Size(79): Append(log_id: T0-N1.0, payload: membership:{voters:[{1:EmptyNode}], learners:[]})
  R-00003: [000_000_125, 000_000_175) Size(50): SaveVote(<T1-N1:->)
  R-00004: [000_000_175, 000_000_225) Size(50): SaveVote(<T1-N1:Q>)
  R-00005: [000_000_225, 000_000_277) Size(52): Append(log_id: T1-N1.1, payload: blank)
  R-00006: [000_000_277, 000_000_472) Size(195): Append(log_id: T1-N1.2, payload: normal:time: <TIME> cmd: add_node(no-override):1=id=1 raft=localhost:28103 grpc=127.0.0.1:9191)
  R-00007: [000_000_472, 000_000_518) Size(46): Commit(T1-N1.2)
  R-00008: [000_000_518, 000_000_597) Size(79): Append(log_id: T1-N1.3, payload: membership:{voters:[{1:EmptyNode}], learners:[]})
  R-00009: [000_000_597, 000_000_643) Size(46): Commit(T1-N1.3)
  R-00010: [000_000_643, 000_000_838) Size(195): Append(log_id: T1-N1.4, payload: normal:time: <TIME> cmd: add_node(override):1=id=1 raft=localhost:28103 grpc=127.0.0.1:9191)
  R-00011: [000_000_838, 000_000_884) Size(46): Commit(T1-N1.4)
  R-00012: [000_000_884, 000_001_146) Size(262): Append(log_id: T1-N1.5, payload: normal:time: <TIME> cmd: txn:TxnRequest{if:[app/db/host >= seq(0)] then:[Put(Put key=app/db/host)] else:[Get(Get key=app/db/host)]})
  R-00013: [000_001_146, 000_001_192) Size(46): Commit(T1-N1.5)
  R-00014: [000_001_192, 000_001_449) Size(257): Append(log_id: T1-N1.6, payload: normal:time: <TIME> cmd: txn:TxnRequest{if:[app/db/port >= seq(0)] then:[Put(Put key=app/db/port)] else:[Get(Get key=app/db/port)]})
  R-00015: [000_001_449, 000_001_495) Size(46): Commit(T1-N1.6)
  R-00016: [000_001_495, 000_001_771) Size(276): Append(log_id: T1-N1.7, payload: normal:time: <TIME> cmd: txn:TxnRequest{if:[app/config/timeout >= seq(0)] then:[Put(Put key=app/config/timeout)] else:[Get(Get key=app/config/timeout)]})
  R-00017: [000_001_771, 000_001_817) Size(46): Commit(T1-N1.7)"""

    assert_match_any(result, [expected, expected2, expected3])

    # Clean up only on success
    shutil.rmtree(".databend", ignore_errors=True)


def test_dump_raft_log_wal_decode_values():
    """Test dump-raft-log-wal --decode-values with real WAL data containing __fd_* keys."""
    print_title("Test dump-raft-log-wal --decode-values (static WAL)")

    data_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "data", "dump_raft_log_wal"
    )

    result = run_command(
        [
            metactl_bin,
            "dump-raft-log-wal",
            "--raft-dir",
            data_dir,
            "--decode-values",
        ]
    )

    expected_file = os.path.join(data_dir, "expected_decoded.txt")
    with open(expected_file) as f:
        expected = f.read()

    actual_masked = mask_time(result.strip())
    # The full WAL output is ~1900 lines, but the tail is repetitive
    # cluster heartbeat renewals. The first 1200 lines cover all
    # distinct record types (DatabaseMeta, RoleInfo, ClientSession, etc.).
    actual_lines = actual_masked.split("\n")[:1200]

    success, error_msg = compare_output(actual_lines, expected.strip())
    if success:
        print(f"All {len(actual_lines)} lines match expected output")
    else:
        assert False, error_msg


def main():
    """Main function to run all dump-raft-log-wal tests."""
    test_dump_raft_log_wal()
    test_dump_raft_log_wal_decode_values()


if __name__ == "__main__":
    main()
