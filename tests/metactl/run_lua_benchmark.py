#!/usr/bin/env python3

import sys
import shutil
import subprocess
import select
from pathlib import Path
from metactl_utils import metactl_bin
from utils import print_title, print_step, kill_databend_meta, start_meta_node


def setup_meta_service():
    """Setup meta service for benchmarking."""
    print_step("Setting up meta service")

    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    print_step("Meta service ready at 127.0.0.1:9191")
    return "127.0.0.1:9191"


def run_lua_script(script_path):
    """Run the Lua script with metactl."""
    print_step(f"Running Lua script: {script_path}")

    script_file = Path(script_path)
    if not script_file.exists():
        raise FileNotFoundError(f"Lua script not found: {script_path}")

    try:
        process = subprocess.Popen([
            metactl_bin, "lua", "--file", str(script_file)
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=0)

        while process.poll() is None:
            ready, _, _ = select.select([process.stdout, process.stderr], [], [], 0.1)
            
            for stream in ready:
                line = stream.readline()
                if line:
                    if stream == process.stdout:
                        print(line.rstrip())
                    else:
                        print("STDERR:", line.rstrip(), file=sys.stderr)
        
        for line in process.stdout:
            print(line.rstrip())
        for line in process.stderr:
            print("STDERR:", line.rstrip(), file=sys.stderr)

        return_code = process.wait()
        if return_code != 0:
            raise subprocess.CalledProcessError(return_code, [metactl_bin, "lua", "--file", str(script_file)])

    except subprocess.CalledProcessError as e:
        print(f"Error running Lua script: {e}")
        raise


def main():
    """Main function to run Lua benchmark with meta service setup."""
    if len(sys.argv) != 2:
        print("Usage: python run_lua_benchmark.py <lua_script_file>")
        print("Example: python run_lua_benchmark.py databend_meta_benchmark.lua")
        sys.exit(1)

    script_path = sys.argv[1]
    script_name = Path(script_path).name

    print_title(f"Running Lua Benchmark: {script_name}")

    grpc_addr = setup_meta_service()
    run_lua_script(script_path)

    print_step("Benchmark completed successfully")


if __name__ == "__main__":
    main()
