#!/usr/bin/env python3

import os
import subprocess
import time
import sys
import socket
from pathlib import Path

BUILD_PROFILE = os.environ.get("BUILD_PROFILE", "debug")
SCRIPT_PATH = Path(__file__).parent.absolute()


def run_command(cmd, check=True, shell=False):
    """Run a command and return its output"""
    if isinstance(cmd, str) and not shell:
        cmd = cmd.split()

    print(f"Running: {cmd}")
    result = subprocess.run(
        cmd,
        check=check,
        shell=shell,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.stderr:
        print(f"STDERR: {result.stderr}")
    if result.returncode != 0:
        print(f"Exit code: {result.returncode}")
    return result.stdout


def wait_for_port(port, timeout=10):
    """Wait for a port to become available"""
    now = time.time()

    while time.time() - now < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(("0.0.0.0", port))
                print("OK :{} is listening".format(port))
                sys.stdout.flush()
                return
        except Exception:
            print("... connecting to :{}".format(port))
            sys.stdout.flush()
            time.sleep(1)

    raise Exception("fail to connect to :{}".format(port))


def kill_databend_meta():
    """Kill all running databend-meta processes"""
    print_step("Kill databend-meta processes")
    try:
        run_command("killall databend-meta", check=False)
        time.sleep(0.5)
    except subprocess.CalledProcessError:
        pass  # It's okay if there are no processes to kill


def start_meta_node(node_id, is_new: bool):
    """Start a databend-meta node with the specified configuration"""
    meta_bin = f"./target/{BUILD_PROFILE}/databend-meta"

    ports = {
        1: (9191, 19191),
        2: (28202, 29191),
        3: (28302, 39191),
    }

    if is_new:
        config_fn = f"new-databend-meta-node-{node_id}.toml"
        port = ports[node_id][1]
    else:
        config_fn = f"databend-meta-node-{node_id}.toml"
        port = ports[node_id][0]

    config_file = f"./tests/metactl/config/{config_fn}"

    subprocess.Popen(
        [meta_bin, "--config-file", config_file],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    wait_for_port(port)
    time.sleep(1)


def print_title(title):
    """Print a formatted title"""
    print()
    print(" ===")
    print(f" === {title}")
    print(" ===")


def print_step(step: str):
    """Print a formatted step message"""
    print(f" === {step}")
