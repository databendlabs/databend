#!/usr/bin/env python3

import glob
import os
import shutil
import time
from metactl_utils import metactl_bin
from utils import run_command, kill_databend_meta, start_meta_node, print_title


def test_trigger_snapshot():
    """Test trigger-snapshot subcommand."""
    print_title("Test trigger snapshot")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    admin_addr = "127.0.0.1:28101"

    # Check snapshot files before trigger
    snapshot_dir = ".databend/meta1/df_meta/V004/snapshot"
    if os.path.exists(snapshot_dir):
        initial_snapshots = glob.glob(f"{snapshot_dir}/*.snap")
    else:
        initial_snapshots = []

    print("Initial_snapshots:", initial_snapshots)

    # Test trigger snapshot
    result = run_command(
        [metactl_bin, "trigger-snapshot", "--admin-api-address", admin_addr]
    )

    assert "triggered snapshot successfully" in result
    print("✓ Trigger snapshot command executed successfully")

    # Give some time for snapshot to be created
    time.sleep(2)

    # Verify snapshot file is generated
    assert os.path.exists(snapshot_dir), (
        f"Snapshot directory does not exist: {snapshot_dir}"
    )

    current_snapshots = glob.glob(f"{snapshot_dir}/*.snap")
    print("Current_snapshots:", current_snapshots)
    assert len(current_snapshots) > len(initial_snapshots), (
        f"No new snapshot file created. Before: {len(initial_snapshots)}, After: {len(current_snapshots)}"
    )

    print(f"✓ Snapshot file created: {len(current_snapshots)} total snapshot(s)")
    print("✓ Trigger snapshot test passed")

    # Clean up only on success
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)


def main():
    """Main function to run all trigger-snapshot tests."""
    test_trigger_snapshot()


if __name__ == "__main__":
    main()
