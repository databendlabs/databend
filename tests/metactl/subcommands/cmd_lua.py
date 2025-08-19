#!/usr/bin/env python3

import subprocess
import tempfile
import os
from metactl_utils import metactl_bin
from utils import print_title


def test_lua_file():
    """Test lua subcommand with file input."""
    print_title("Test lua subcommand with file")

    # Create a temporary Lua script
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lua", delete=False) as f:
        f.write("print(2 + 3)\n")
        lua_file = f.name

    print("file name:", lua_file)
    with open(lua_file, "r") as ff:
        print("file content:", ff.read())

    # Run metactl lua with file
    result = subprocess.run(
        [metactl_bin, "lua", "--file", lua_file],
        capture_output=True,
        text=True,
        check=True,
    )

    output = result.stdout.strip()
    assert "5" == output
    print("✓ Lua file execution test passed")


def test_lua_stdin():
    """Test lua subcommand with stdin input."""
    print_title("Test lua subcommand with stdin")

    lua_script = 'print("Hello from stdin!")\nprint(5 * 6)\n'

    # Run metactl lua with stdin
    result = subprocess.run(
        [metactl_bin, "lua"],
        input=lua_script,
        capture_output=True,
        text=True,
        check=True,
    )

    output = result.stdout.strip()
    assert "Hello from stdin!" in output
    assert "30" in output
    print("✓ Lua stdin execution test passed")


def main():
    """Main function to run all lua tests."""
    test_lua_file()
    test_lua_stdin()


if __name__ == "__main__":
    main()
