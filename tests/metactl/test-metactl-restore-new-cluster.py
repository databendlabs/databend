#!/usr/bin/env python3

import json
import os
import shutil
import subprocess
import time
import sys
import re
import signal
from typing import Dict
import requests
from pathlib import Path
import socket

# Configuration
BUILD_PROFILE = os.environ.get("BUILD_PROFILE", "debug")
SCRIPT_PATH = Path(__file__).parent.absolute()
metactl_bin = f"./target/{BUILD_PROFILE}/databend-metactl"


# Helper functions
def run_command(cmd, check=True, shell=False):
    """Run a command and return its output"""
    if isinstance(cmd, str) and not shell:
        cmd = cmd.split()

    print(f"Running: {cmd}")
    result = subprocess.run(cmd,
                            check=check, shell=shell,
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout


def wait_for_port(port, timeout=10):
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
    print_step("Kill databend-meta processes")
    try:
        run_command("killall databend-meta", check=False)
        time.sleep(2)
    except subprocess.CalledProcessError:
        pass  # It's okay if there are no processes to kill


def start_meta_node(node_id, is_new: bool):
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

    subprocess.Popen([meta_bin, "--config-file", config_file],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    wait_for_port(port)
    time.sleep(1)


# return the stdout of metactl export
def metactl_export(meta_dir: str, output_path: str) -> str:
    print_step(f"Start: Export meta data from {meta_dir} to {output_path}")

    cmd = [metactl_bin, "export", "--raft-dir", meta_dir]

    if output_path:
        cmd.append("--db")
        cmd.append(output_path)

    result = subprocess.Popen(cmd,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result.wait()

    print_step(f"Done: Exported meta data to {output_path}")
    return result.stdout.read().decode()


def metactl_import(meta_dir: str, id: int, db_path: str, initial_cluster: Dict[int, str]):
    cmd = [metactl_bin, "import", "--raft-dir", meta_dir,
           "--id", str(id),
           "--db", db_path, ]

    for id, addr in initial_cluster.items():
        cmd.append("--initial-cluster")
        cmd.append(f"{id}={addr}")

    result = subprocess.Popen(cmd,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result.wait()
    print(result.stdout.read().decode())
    print(result.stderr.read().decode())


def cluster_status(msg: str) -> str:
    print_step(f"Check /v1/cluster/status {msg} start")

    response = requests.get("http://127.0.0.1:28101/v1/cluster/status")
    status = response.json()

    print_step(f"Check /v1/cluster/status {msg} end")

    return status


def print_title(title):
    print()
    print(" ===")
    print(f" === {title}")
    print(" ===")


def print_step(step: str):
    print(f" === {step}")


def main():
    kill_databend_meta()

    shutil.rmtree(".databend", ignore_errors=True)

    print_title("1. Start 3 meta node cluster to generate raft logs")

    start_meta_node(1, False)
    start_meta_node(2, False)
    start_meta_node(3, False)

    old_status = cluster_status("old cluster")
    print(json.dumps(old_status, indent=2))

    kill_databend_meta()

    print_title("2. Export meta node data")

    exported = metactl_export("./.databend/meta1", None)

    print_step("Exported meta data. start")
    print(exported)
    print_step("Exported meta data. end")

    print_step("Clear meta service data dirs")
    shutil.rmtree(".databend")

    print_title("3. Import old meta node data to new cluster")

    cluster = {
        4: "localhost:29103",
        5: "localhost:29203",
        6: "localhost:29303",
    }

    metactl_import("./.databend/new_meta1", 4, "meta.db", cluster)
    metactl_import("./.databend/new_meta2", 5, "meta.db", cluster)
    metactl_import("./.databend/new_meta3", 6, "meta.db", cluster)

    print_step("3.1 Check if state machine is complete by checking key 'LastMembership'")

    meta1_data = metactl_export("./.databend/new_meta1", None)
    print(meta1_data)

    meta2_data = metactl_export("./.databend/new_meta2", None)
    print(meta2_data)

    meta3_data = metactl_export("./.databend/new_meta3", None)
    print(meta3_data)

    assert "LastMembership" in meta1_data
    assert "LastMembership" in meta2_data
    assert "LastMembership" in meta3_data

    print_title("3.2 Start 3 new meta node cluster")

    start_meta_node(1, True)
    start_meta_node(2, True)
    start_meta_node(3, True)

    print_step("sleep 5 sec to wait for membership to commit")
    time.sleep(5)

    print_title("3.3 Check membership in new cluster")

    new_status = cluster_status("new cluster")
    print(json.dumps(new_status, indent=2))

    voters = new_status["voters"]

    names = dict([(voter["name"], voter) for voter in voters])

    assert names["4"]["endpoint"] == {"addr": "localhost", "port": 29103}
    assert names["5"]["endpoint"] == {"addr": "localhost", "port": 29203}
    assert names["6"]["endpoint"] == {"addr": "localhost", "port": 29303}

    kill_databend_meta()
    shutil.rmtree(".databend")


    print_title("4. Import with --initial-cluster of one node")

    cluster = {
        # This will be replaced when meta node starts
        4: "127.0.0.1:12345",
    }

    metactl_import("./.databend/new_meta1", 4, "meta.db", cluster)

    print_step("4.1 Check if state machine is complete by checking key 'LastMembership'")
    meta1_data = metactl_export("./.databend/new_meta1", None)
    print(meta1_data)
    assert "LastMembership" in meta1_data

    print_title("4.2 Start new meta node cluster")

    start_meta_node(1, True)

    print_step("sleep 3 sec to wait for membership to commit")
    time.sleep(3)

    print_title("4.3 Check membership in new cluster")

    new_status = cluster_status("new single node cluster")
    print(json.dumps(new_status, indent=2))

    voters = new_status["voters"]

    names = dict([(voter["name"], voter) for voter in voters])

    # The address is replaced with the content in config.
    assert names["4"]["endpoint"] == {"addr": "localhost", "port": 29103}

    kill_databend_meta()
    shutil.rmtree(".databend")

if __name__ == "__main__":
    main()
