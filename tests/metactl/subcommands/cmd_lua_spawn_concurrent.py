#!/usr/bin/env python3

import subprocess
import tempfile
import os
import shutil
from metactl_utils import metactl_bin, metactl_run_lua
from utils import print_title, kill_databend_meta, start_meta_node


def test_spawn_basic():
    """Test basic spawn functionality without gRPC."""
    print_title("Test basic spawn functionality")

    lua_script = f"""
print("Testing basic spawn functionality...")

local task1 = metactl.spawn(function()
    print("Task 1: Starting")
    metactl.sleep(0.1)
    print("Task 1: Finished")
end)

local task2 = metactl.spawn(function()
    print("Task 2: Starting")
    metactl.sleep(0.2)
    print("Task 2: Finished")
end)

print("Created both tasks, now joining...")

task1:join()
print("Task 1 joined")

task2:join()
print("Task 2 joined")

print("All tasks completed!")
"""

    output = metactl_run_lua(lua_script=lua_script)

    expected_phrases = [
        "Testing basic spawn functionality...",
        "Task 1: Starting",
        "Task 2: Starting",
        "Created both tasks, now joining...",
        "Task 1: Finished",
        "Task 1 joined",
        "Task 2: Finished",
        "Task 2 joined",
        "All tasks completed!",
    ]

    for phrase in expected_phrases:
        assert phrase in output, (
            f"Expected phrase '{phrase}' not found in output:\n{output}"
        )
    print("✓ Basic spawn functionality test passed")


def main():
    """Test spawn functionality with multiple concurrent tasks."""
    test_spawn_basic()


if __name__ == "__main__":
    main()
