#!/usr/bin/env python3
"""Secured metactl suite: strict gRPC authentication over plaintext and over TLS.

A profile starts one databend-meta node, runs every distinct gRPC client
creation path with a valid credential, and runs the authentication failure
matrix once against `get`. The TLS profile also runs the TLS failure matrix.
Admin HTTP commands are out of scope.
"""

import dataclasses
import json
import subprocess
import time
from pathlib import Path

from metactl_utils import Metactl, client_for, metactl_trigger_snapshot
from utils import (
    CURRENT_CREDENTIAL,
    NEXT_CREDENTIAL,
    STRICT_AUTH,
    STRICT_AUTH_TLS,
    UNRELATED_CA_CERT,
    LocalMetaCluster,
    MetaClientProfile,
    MetaNodePorts,
    MetaSecurityProfile,
    build_meta_node,
    meta_cluster,
    print_step,
    print_title,
    write_password_file,
)

WORK_DIR = Path(".databend/metactl-secured")
METACTL_LOG_DIR = Path(".databend/logs")
PORTS = MetaNodePorts(admin=28701, grpc=28702, raft=28703)
WRONG_PASSWORD = "wrong-password"
PASSWORDS = (CURRENT_CREDENTIAL.password, NEXT_CREDENTIAL.password, WRONG_PASSWORD)
# databend-meta answers every rejected credential with this gRPC status.
AUTH_ERROR = "does not have valid authentication credentials"
TLS_PAIR_ERROR = "--grpc-tls-ca-cert and --grpc-tls-domain-name must be set together"
WATCH_SETTLE_SEC = 2
SNAPSHOT_SETTLE_SEC = 2
KEY = "secured/key"
VALUE = "secured-value"


def assert_no_password(text: str, where: str) -> None:
    for password in PASSWORDS:
        assert password not in text, f"a password leaked into {where}"


def run(metactl: Metactl, args: list[str]) -> subprocess.CompletedProcess:
    """Run one command; no password may appear in its output."""
    result = metactl.run(args)
    assert_no_password(result.stdout + result.stderr, f"the output of {args}")
    return result


def run_ok(metactl: Metactl, args: list[str]) -> str:
    """Run one command that must succeed; return its stdout."""
    result = run(metactl, args)
    assert result.returncode == 0, f"{args} failed: {result.stderr}"
    return result.stdout


def run_rejected(metactl: Metactl, args: list[str]) -> str:
    """Run one command the server must reject as unauthenticated; return its stderr."""
    result = run(metactl, args)
    assert result.returncode != 0, f"{args} succeeded with {metactl.client}"
    assert AUTH_ERROR in result.stderr, f"{args} failed otherwise: {result.stderr}"
    return result.stderr


def run_not_connected(metactl: Metactl, args: list[str]) -> str:
    """Run one command that must fail before the server checks its credential."""
    result = run(metactl, args)
    assert result.returncode != 0, f"{args} succeeded with {metactl.client}"
    assert AUTH_ERROR not in result.stderr, f"{args} was authenticated: {result.stderr}"
    return result.stderr


def check_client_paths(
    metactl: Metactl, address: str, admin_address: str, work_dir: Path
) -> None:
    """Every distinct gRPC client creation path works with a valid credential."""
    print_step("status")
    status = run_ok(metactl, ["status"])
    assert "BinaryVersion" in status, status

    print_step("upsert then get")
    run_ok(metactl, ["upsert", "--key", KEY, "--value", VALUE])
    printed = run_ok(metactl, ["get", "--key", KEY])
    got = json.loads(printed)
    value = bytes(got["data"]).decode()
    assert value == VALUE

    print_step("export from the running node")
    exported = run_ok(metactl, ["export"])
    assert KEY in exported, exported

    print_step("legacy --export entry point")
    exported = run_ok(metactl, ["--export"])
    assert KEY in exported, exported

    print_step("keys-layout")
    # keys-layout reads the last snapshot; a fresh node has none.
    metactl_trigger_snapshot(admin_address)
    time.sleep(SNAPSHOT_SETTLE_SEC)
    layout = run_ok(metactl, ["keys-layout"])
    assert "secured" in layout, layout

    print_step("member-list")
    members = run_ok(metactl, ["member-list"])
    assert address in members, members

    print_step("lua with metactl.new_grpc_client")
    script = work_dir / "get.lua"
    script.write_text(
        f'local client = metactl.new_grpc_client("{address}")\n'
        f'local result, err = client:get("{KEY}")\n'
        "assert(err == nil, tostring(err))\n"
        "print(metactl.to_string(result))\n"
    )
    printed = run_ok(metactl, ["lua", "--file", str(script)])
    assert VALUE in printed, printed

    print_step("watch")
    watch = metactl.start(["watch", "--prefix", "secured/"])
    time.sleep(WATCH_SETTLE_SEC)
    run_ok(metactl, ["upsert", "--key", "secured/watched", "--value", "1"])
    time.sleep(WATCH_SETTLE_SEC)
    assert watch.poll() is None, "watch exited early"
    watch.terminate()
    stdout, stderr = watch.communicate(timeout=5)
    assert_no_password(stdout + stderr, "the watch output")
    assert "secured/watched" in stdout, stdout

    print_step("bench-client-num-conn")
    bench = run_ok(metactl, ["bench-client-num-conn", "--num", "2"])
    assert "bench completed: 2 connections created" in bench, bench


def check_credential_rotation(
    address: str, server: MetaSecurityProfile, secrets: Path
) -> None:
    print_step("the next credential succeeds")
    rotated = Metactl(address, client_for(server, NEXT_CREDENTIAL, secrets))
    run_ok(rotated, ["status"])


def check_authentication_failures(
    address: str, valid: MetaClientProfile, secrets: Path
) -> None:
    """The failure matrix against `get`; every rejected client derives from `valid`."""
    get = ["get", "--key", KEY]

    print_step("missing password is rejected")
    missing = dataclasses.replace(valid, password_file=None)
    run_rejected(Metactl(address, missing), get)

    print_step("wrong password is rejected")
    wrong_file = write_password_file(secrets / "wrong.password", WRONG_PASSWORD)
    wrong = dataclasses.replace(valid, password_file=wrong_file)
    run_rejected(Metactl(address, wrong), get)

    print_step("unknown user is rejected")
    unknown = dataclasses.replace(valid, username="nobody")
    stderr = run_rejected(Metactl(address, unknown), get)
    assert "Unknown user: nobody" in stderr, stderr

    print_step("anonymous client is rejected")
    anonymous = dataclasses.replace(valid, username=None, password_file=None)
    run_rejected(Metactl(address, anonymous), get)

    print_step("bench-client-num-conn fails without a credential")
    run_rejected(Metactl(address, anonymous), ["bench-client-num-conn", "--num", "1"])


def check_password_files(address: str, valid: MetaClientProfile, secrets: Path) -> None:
    """`valid` already used a file ending in LF; the other endings and errors."""
    password = CURRENT_CREDENTIAL.password

    print_step("password file ending in CRLF works")
    crlf = secrets / "crlf.password"
    crlf.write_bytes(password.encode() + b"\r\n")
    client = dataclasses.replace(valid, password_file=crlf)
    run_ok(Metactl(address, client), ["status"])

    print_step("password file without a newline works")
    bare = secrets / "bare.password"
    bare.write_text(password)
    client = dataclasses.replace(valid, password_file=bare)
    run_ok(Metactl(address, client), ["status"])

    print_step("missing password file fails before connecting")
    client = dataclasses.replace(valid, password_file=secrets / "missing.password")
    result = run(Metactl(address, client), ["status"])
    assert result.returncode != 0
    assert "failed to read gRPC authentication password" in result.stderr, result.stderr

    print_step("empty password file fails before connecting")
    empty = secrets / "empty.password"
    empty.write_text("")
    client = dataclasses.replace(valid, password_file=empty)
    result = run(Metactl(address, client), ["status"])
    assert result.returncode != 0
    assert "gRPC authentication password must not be empty" in result.stderr, (
        result.stderr
    )


def check_tls_failures(address: str, valid: MetaClientProfile) -> None:
    """The TLS failure matrix against `get`; every rejected client derives from `valid`."""
    get = ["get", "--key", KEY]

    print_step("client without TLS settings is rejected, not downgraded")
    plain = dataclasses.replace(valid, grpc_tls_ca_cert=None, grpc_tls_domain_name=None)
    run_not_connected(Metactl(address, plain), get)

    print_step("CA that did not sign the server certificate is rejected")
    assert UNRELATED_CA_CERT.is_file(), UNRELATED_CA_CERT
    wrong_ca = dataclasses.replace(valid, grpc_tls_ca_cert=UNRELATED_CA_CERT)
    run_not_connected(Metactl(address, wrong_ca), get)

    print_step("domain name outside the server certificate is rejected")
    # server.pem also lists example.com and example.test among its names.
    wrong_domain = dataclasses.replace(valid, grpc_tls_domain_name="bogus.invalid")
    run_not_connected(Metactl(address, wrong_domain), get)

    print_step("CA without a domain name fails before connecting")
    ca_only = dataclasses.replace(valid, grpc_tls_domain_name=None)
    result = run(Metactl(address, ca_only), get)
    assert result.returncode != 0
    assert TLS_PAIR_ERROR in result.stderr, result.stderr


def check_logs(cluster: LocalMetaCluster) -> None:
    """No password in the node's stdout and logs or in metactl's logs.

    The node's TOML config holds the credentials by design and is not scanned.
    """
    print_step("no password in server or metactl logs")
    node_dir = cluster.work_dir / "node-1"
    logs = [node_dir / "stdout.log"]
    logs += [p for p in (node_dir / "logs").rglob("*") if p.is_file()]
    logs += [p for p in METACTL_LOG_DIR.rglob("*") if p.is_file()]
    assert logs, f"no log files under {node_dir} or {METACTL_LOG_DIR}"
    for path in logs:
        assert_no_password(path.read_text(errors="replace"), str(path))


def run_profile(title: str, server: MetaSecurityProfile) -> None:
    print_title(f"Test metactl with {title}")
    node = build_meta_node(1, PORTS, security=server)
    with meta_cluster(WORK_DIR, [node]) as cluster:
        address = cluster.grpc_address(1)
        secrets = cluster.work_dir / "secrets"
        valid = client_for(server, CURRENT_CREDENTIAL, secrets)
        metactl = Metactl(address, valid)
        check_client_paths(metactl, address, cluster.admin_address(1), cluster.work_dir)
        check_credential_rotation(address, server, secrets)
        check_authentication_failures(address, valid, secrets)
        check_password_files(address, valid, secrets)
        if server.grpc_tls_server_cert is not None:
            check_tls_failures(address, valid)
        check_logs(cluster)
    print(f"✓ metactl with {title} passed")


def main():
    run_profile("strict authentication over plaintext gRPC", STRICT_AUTH)
    run_profile("strict authentication over gRPC TLS", STRICT_AUTH_TLS)


if __name__ == "__main__":
    main()
