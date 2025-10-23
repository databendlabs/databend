#!/usr/bin/env python3

import re
import shutil
import time
from metactl_utils import metactl_bin, metactl_upsert, metactl_trigger_snapshot
from utils import run_command, kill_databend_meta, start_meta_node, print_title


def test_dump_raft_log_wal():
    """Test dump-raft-log-wal subcommand from raft directory."""
    print_title("Test dump-raft-log-wal from raft directory")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    grpc_addr = "127.0.0.1:9191"
    admin_addr = "127.0.0.1:28101"

    # Insert test data
    test_keys = [
        ("app/db/host", "localhost"),
        ("app/db/port", "5432"),
        ("app/config/timeout", "30"),
    ]

    for key, value in test_keys:
        metactl_upsert(grpc_addr, key, value)
    print("✓ Test data inserted")

    # Trigger snapshot to ensure raft log has data
    metactl_trigger_snapshot(admin_addr)
    print("✓ Snapshot triggered")

    # Wait for snapshot to complete
    time.sleep(2)

    # Stop meta service before accessing raft directory
    kill_databend_meta()

    # Test dump-raft-log-wal from raft directory
    result = run_command([metactl_bin, "dump-raft-log-wal", "--raft-dir", "./.databend/meta1"])

    print("Output:")
    print(result)

    # Expected output with time field that will be masked
    expected = """RaftLog:
ChunkId(00_000_000_000_000_000_000)
  R-00000: [000_000_000, 000_000_018) Size(18): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: None))
  R-00001: [000_000_018, 000_000_046) Size(28): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: LogStoreMeta{ node_id: Some(1) }))
  R-00002: [000_000_046, 000_000_125) Size(79): Append(log_id: T0-N0.0, payload: membership:{voters:[{1:EmptyNode}], learners:[]})
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

    def mask_time(text):
        """Mask time fields in the output for comparison."""
        # Mask timestamp in format "time: 2025-10-19T15:03:55.193"
        text = re.sub(r'time: \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+', 'time: <TIME>', text)
        return text

    # Normalize both actual and expected output
    actual_lines = result.strip().split("\n")
    expected_lines = expected.strip().split("\n")

    # Mask time in actual output
    actual_masked = [mask_time(line) for line in actual_lines]

    # Verify line count matches
    assert len(actual_masked) == len(expected_lines), (
        f"Line count mismatch: got {len(actual_masked)} lines, expected {len(expected_lines)} lines"
    )

    # Compare line by line
    for i, (actual_line, expected_line) in enumerate(zip(actual_masked, expected_lines)):
        assert actual_line == expected_line, (
            f"Line {i} mismatch:\n"
            f"  Actual  : {actual_line}\n"
            f"  Expected: {expected_line}"
        )

    print(f"✓ All {len(actual_masked)} lines match expected output (time fields masked)")

    # Clean up only on success
    shutil.rmtree(".databend", ignore_errors=True)


def main():
    """Main function to run all dump-raft-log-wal tests."""
    test_dump_raft_log_wal()


if __name__ == "__main__":
    main()
