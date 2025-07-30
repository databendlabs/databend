#!/usr/bin/env python3

import subprocess
import tempfile
import os
import shutil
import json
from metactl_utils import metactl_bin
from utils import print_title, kill_databend_meta, start_meta_node

def load_lua_util():
    with open("tests/metactl/lua_util.lua", 'r') as f:
        return f.read()


def setup_test_environment():
    """Setup meta service for testing."""
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)
    return "127.0.0.1:9191"


def test_lua_grpc_client():
    """Test lua subcommand with gRPC client functionality."""
    print_title("Test lua subcommand with gRPC client")

    # Setup meta service
    grpc_addr = setup_test_environment()

    lua_util_str = load_lua_util()

    # Create a Lua script that uses the gRPC client
    lua_script = f'''
{lua_util_str}

local client = new_grpc_client("{grpc_addr}")

-- Test upsert operation
local upsert_result, upsert_err = client:upsert("test_key", "test_value")
if upsert_err then
    print("Upsert error:", upsert_err)
else
    print("Upsert result:", to_string(upsert_result))
end

-- Test get operation
local get_result, get_err = client:get("test_key")
if get_err then
    print("Get error:", get_err)
else
    print("Get result:", to_string(get_result))
end

-- Test get non-existent key
local get_null, get_null_err = client:get("nonexistent_key")
if get_null_err then
    print("Get null error:", get_null_err)
else
    print("Get null result:", to_string(get_null))
end
'''

    # Run metactl lua with gRPC client script
    result = subprocess.run([
        metactl_bin, "lua"
    ], input=lua_script, capture_output=True, text=True, check=True)

    output = result.stdout.strip()
    print("output:", output)

    expected_output = '''Upsert result:\t{"result"={"data"="test_value","seq"=1}}
Get result:\t{"data"="test_value","seq"=1}
Get null result:\tNULL'''
    print("expect:", expected_output)

    # Check if entire output matches expected value
    assert output == expected_output, f"Expected:\n{expected_output}\n\nActual:\n{output}"

    print("✓ Lua gRPC client test passed")

    # Only cleanup on success
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)


def main():
    """Main function to run all lua gRPC tests."""
    test_lua_grpc_client()


if __name__ == "__main__":
    main()
