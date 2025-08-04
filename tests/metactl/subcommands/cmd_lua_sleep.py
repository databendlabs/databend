#!/usr/bin/env python3

import subprocess
import time
from metactl_utils import metactl_bin
from utils import print_title

def test_lua_sleep():
    """Test sleep functionality."""
    print_title("Test sleep functionality")

    lua_script = '''
print("Before sleep")
metactl.sleep(0.5)
print("After sleep")
'''

    start_time = time.time()

    result = subprocess.run([
        metactl_bin, "lua"
    ], input=lua_script, capture_output=True, text=True, check=True)

    execution_time = time.time() - start_time

    expected_output = "Before sleep\nAfter sleep"
    assert result.stdout.strip() == expected_output

    # Verify timing
    assert execution_time >= 0.5, f"Expected >= 0.5s, got {execution_time:.2f}s"
    assert execution_time < 2.0, f"Expected < 2.0s, got {execution_time:.2f}s"

    print(f"✓ Sleep test passed ({execution_time:.2f}s)")


def main():
    """Main function to run lua sleep test."""
    test_lua_sleep()


if __name__ == "__main__":
    main()
