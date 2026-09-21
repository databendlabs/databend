#!/usr/bin/env python3

import dataclasses
import os
from collections.abc import Mapping
import socket
import subprocess
import sys
import time
from pathlib import Path

BUILD_PROFILE = os.environ.get("BUILD_PROFILE", "debug")
SCRIPT_PATH = Path(__file__).parent.absolute()
REPO_PATH = SCRIPT_PATH.parent.parent
META_BINARY = REPO_PATH / "target" / BUILD_PROFILE / "databend-meta"
METACTL_BINARY = REPO_PATH / "target" / BUILD_PROFILE / "databend-metactl"

CERTS_DIR = REPO_PATH / "tests" / "certs"
TEST_SERVER_CERT = CERTS_DIR / "server.pem"
TEST_SERVER_KEY = CERTS_DIR / "server.key"
TEST_CA_CERT = CERTS_DIR / "ca.pem"
# A CA that did not sign server.pem, for negative TLS tests.
UNRELATED_CA_CERT = CERTS_DIR / "tls" / "cfssl" / "ca" / "ca.pem"
# server.pem lists localhost and 127.0.0.1 as subject alternative names.
TEST_TLS_DOMAIN = "localhost"

sys.path.insert(0, str(REPO_PATH / "scripts" / "databend_test_helper" / "src"))
from databend_test_helper import (  # noqa: E402
    LocalMetaCluster,
    LocalMetaNode,
    MetaClientProfile as MetaClientProfile,
    MetaGrpcCredential,
    MetaNodePorts,
    MetaSecurityProfile,
    render_meta_config,
    write_password_file as write_password_file,
)


def build_meta_node(
    node_id: int,
    ports: MetaNodePorts,
    security: MetaSecurityProfile = MetaSecurityProfile(),
    join_addresses: tuple[str, ...] = (),
    raft_settings: Mapping[str, object] | None = None,
    meta_bin: Path = META_BINARY,
) -> LocalMetaNode:
    """Describe one test node; its files live under `node-{node_id}/` in the work dir."""
    node_dir = Path(f"node-{node_id}")
    config_text = render_meta_config(
        node_id,
        ports,
        raft_dir=node_dir / "raft",
        log_dir=node_dir / "logs",
        security=security,
        join_addresses=join_addresses,
        raft_settings=raft_settings,
    )
    return LocalMetaNode(
        node_id=node_id,
        meta_bin=meta_bin,
        ports=ports,
        config_path=node_dir / "databend-meta.toml",
        config_text=config_text,
        stdout_path=node_dir / "stdout.log",
    )


def meta_cluster(work_dir, nodes, start_timeout=10) -> LocalMetaCluster:
    """A cluster of test nodes in a fresh work dir, kept only when the `with` block fails."""
    return LocalMetaCluster(
        list(nodes),
        Path(work_dir),
        reset_work_dir=True,
        cleanup_work_dir_on_success=True,
        start_timeout=start_timeout,
    )


CURRENT_CREDENTIAL = MetaGrpcCredential("meta-current", "current-password")
NEXT_CREDENTIAL = MetaGrpcCredential("meta-next", "next-password")

# Strict gRPC authentication over plaintext gRPC. Two credentials, so a test
# can rotate from CURRENT_CREDENTIAL to NEXT_CREDENTIAL without a restart.
STRICT_AUTH = MetaSecurityProfile(
    grpc_auth_strict=True,
    grpc_credentials=(CURRENT_CREDENTIAL, NEXT_CREDENTIAL),
)

# STRICT_AUTH over gRPC TLS with the certificate under tests/certs. Extend it
# with dataclasses.replace() for Raft TLS and a strict Raft secret.
STRICT_AUTH_TLS = dataclasses.replace(
    STRICT_AUTH,
    grpc_tls_server_cert=TEST_SERVER_CERT,
    grpc_tls_server_key=TEST_SERVER_KEY,
)

RAFT_SECRET = "raft-secret"

# Raft TLS between the nodes with the certificate under tests/certs, and a
# strict shared Raft secret. gRPC stays plain and unauthenticated.
RAFT_TLS_STRICT = MetaSecurityProfile(
    raft_tls_server_cert=TEST_SERVER_CERT,
    raft_tls_server_key=TEST_SERVER_KEY,
    raft_tls_client_root_ca_cert=TEST_CA_CERT,
    raft_tls_client_domain_name=TEST_TLS_DOMAIN,
    raft_secret=RAFT_SECRET,
    raft_accepted_secrets=(RAFT_SECRET,),
    raft_secret_strict=True,
)


def run_command_result(cmd, shell=False):
    """Run a command and return its complete result."""
    if isinstance(cmd, str) and not shell:
        cmd = cmd.split()

    print(f"Running: {cmd}")
    result = subprocess.run(
        cmd,
        check=False,
        shell=shell,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    print(result)

    return result


def run_command(cmd, check=True, shell=False):
    """Run a command and return its stdout."""
    result = run_command_result(cmd, shell=shell)

    if check:
        result.check_returncode()

    if result.stderr:
        print(f"STDERR: {result.stderr}")
    return result.stdout


def wait_for_port(port, timeout=10):
    """Wait for a port to become available"""
    now = time.time()

    while time.time() - now < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(("0.0.0.0", port))
                print("OK :{} is listening".format(port))
                return
        except Exception:
            print("... connecting to :{}".format(port))
            time.sleep(0.3)

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
    time.sleep(0.3)


def print_title(title):
    """Print a formatted title"""
    print()
    print(" ===")
    print(f" === {title}")
    print(" ===")


def print_step(step: str):
    """Print a formatted step message"""
    print(f" === {step}")
