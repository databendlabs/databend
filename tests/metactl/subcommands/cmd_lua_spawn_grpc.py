#!/usr/bin/env python3

import subprocess
import shutil
from metactl_utils import metactl_bin
from utils import print_title, kill_databend_meta, start_meta_node


def setup_test_environment():
    """Setup meta service for testing."""
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)
    return "127.0.0.1:9191"


def test_grpc_cross_task_access():
    """Test gRPC cross-task key access."""
    print_title("Test gRPC cross-task key access")

    grpc_addr = setup_test_environment()

    lua_script = f"""
local task1 = metactl.spawn(function()
    local client = metactl.new_grpc_client("{grpc_addr}")
    
    client:upsert("k1", "v1")
    print("Task 1: Upserted k1")
    
    metactl.sleep(0.5)
    
    local result, err = client:get("k2")
    if not err then
        print("Task 1: Got k2:", metactl.to_string(result))
    end
end)

local task2 = metactl.spawn(function()
    local client = metactl.new_grpc_client("{grpc_addr}")

    -- Let task1 run first
    metactl.sleep(0.2)
    
    client:upsert("k2", "v2")
    print("Task 2: Upserted k2")
    
    metactl.sleep(0.5)
    
    local result, err = client:get("k1")
    if not err then
        print("Task 2: Got k1:", metactl.to_string(result))
    end
end)

task1:join()
task2:join()
print("Done")
"""

    result = subprocess.run(
        [metactl_bin, "lua"],
        input=lua_script,
        capture_output=True,
        text=True,
        check=True,
    )

    output = result.stdout.strip()

    # Normalize output by removing proposed_at_ms field
    import re
    normalized_output = re.sub(r',"meta"=\{"proposed_at_ms"=\d+\}', '', output)

    expected_phrases = [
        "Task 1: Upserted k1",
        "Task 2: Upserted k2",
        'Task 1: Got k2:\t{"data"="v2","seq"=2}',
        'Task 2: Got k1:\t{"data"="v1","seq"=1}',
        "Done",
    ]

    for phrase in expected_phrases:
        assert phrase in normalized_output, (
            f"Expected phrase '{phrase}' not found in output:\n{normalized_output}"
        )

    print("✓ Cross-task access test passed")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)


def main():
    """Test gRPC cross-task access."""
    test_grpc_cross_task_access()


if __name__ == "__main__":
    main()
