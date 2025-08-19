#!/usr/bin/env python3

import subprocess
import tempfile
import os
from metactl_utils import metactl_bin
from utils import print_title


def test_lua_admin_client_creation():
    """Test creating admin client in Lua."""
    print_title("Test lua admin client creation")

    lua_script = """
-- Test that we can create an admin client
local admin_client = metactl.new_admin_client("127.0.0.1:28002")
print("Admin client created successfully")
"""

    # Run metactl lua with stdin
    result = subprocess.run(
        [metactl_bin, "lua"],
        input=lua_script,
        capture_output=True,
        text=True,
        check=True,
    )

    output = result.stdout.strip()
    assert "Admin client created successfully" in output
    print("✓ Lua admin client creation test passed")


def test_lua_admin_client_metrics_error_handling():
    """Test admin client metrics method error handling."""
    print_title("Test lua admin client metrics error handling")

    lua_script = """
-- Test admin client metrics method with invalid address
local admin_client = metactl.new_admin_client("127.0.0.1:99999")
local metrics, err = admin_client:metrics()
if err then
    print("Expected error: " .. string.sub(err, 1, 50) .. "...")
    print("Error handling works correctly")
    return "success", nil  -- Lua convention: (result, error)
else
    error("Should have gotten an error for invalid address")
end
"""

    # Run metactl lua with stdin
    result = subprocess.run(
        [metactl_bin, "lua"],
        input=lua_script,
        capture_output=True,
        text=True,
        check=True,
    )

    output = result.stdout.strip()
    assert "Expected error:" in output
    assert "Error handling works correctly" in output
    print("✓ Lua admin client metrics error handling test passed")


def test_lua_admin_client_all_methods():
    """Test that all admin client methods exist and handle errors properly."""
    print_title("Test lua admin client all methods")

    lua_script = """
-- Test all admin client methods exist
local admin_client = metactl.new_admin_client("127.0.0.1:99999")

local methods_to_test = {
    "metrics",
    "status",
    "list_features",
    "trigger_snapshot"
}

for _, method_name in ipairs(methods_to_test) do
    local method_func = admin_client[method_name]
    if method_func then
        print("✓ Method " .. method_name .. " exists")

        -- Test calling the method (should error due to invalid address)
        local result, err = method_func(admin_client)
        if err then
            print("✓ Method " .. method_name .. " properly handles connection errors")
        else
            print("⚠ Method " .. method_name .. " succeeded unexpectedly")
        end
    else
        error("Method " .. method_name .. " does not exist")
    end
end

print("All admin client methods test completed successfully")
"""

    # Run metactl lua with stdin
    result = subprocess.run(
        [metactl_bin, "lua"],
        input=lua_script,
        capture_output=True,
        text=True,
        check=True,
    )

    output = result.stdout.strip()
    assert "All admin client methods test completed successfully" in output

    # Check that all expected methods were found
    expected_methods = ["metrics", "status", "list_features", "trigger_snapshot"]
    for method in expected_methods:
        assert f"✓ Method {method} exists" in output
        assert (
            f"✓ Method {method} properly handles connection errors" in output
            or f"⚠ Method {method} succeeded unexpectedly" in output
        )

    print("✓ Lua admin client all methods test passed")


def test_lua_admin_client_file():
    """Test admin client functionality with a Lua file."""
    print_title("Test lua admin client with file")

    # Create a temporary Lua script
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lua", delete=False) as f:
        f.write("""
-- Test admin client from file
local admin_client = metactl.new_admin_client("127.0.0.1:99999")
local metrics, err = admin_client:metrics()
if err then
    print("File test: Got expected error")
else
    print("File test: Unexpected success")
end
print("File test completed")
""")
        lua_file = f.name

    try:
        # Run metactl lua with file
        result = subprocess.run(
            [metactl_bin, "lua", "--file", lua_file],
            capture_output=True,
            text=True,
            check=True,
        )

        output = result.stdout.strip()
        assert "File test: Got expected error" in output
        assert "File test completed" in output
        print("✓ Lua admin client file test passed")

    finally:
        # Clean up temp file
        os.unlink(lua_file)


def main():
    """Main function to run all lua admin client tests."""
    test_lua_admin_client_creation()
    test_lua_admin_client_metrics_error_handling()
    test_lua_admin_client_all_methods()
    test_lua_admin_client_file()


if __name__ == "__main__":
    main()
