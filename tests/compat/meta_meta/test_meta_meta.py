#!/usr/bin/env python3
"""
Assert that latest databend-meta is compatible with old version databend-meta.
Tests leader-follower replication and data consistency across versions.
"""

import argparse
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


def wait_tcp_port(port: int, timeout: int = 20) -> None:
    """Wait for TCP port to become available."""
    print(f" === Waiting for port {port} (timeout: {timeout}s)")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                sock.connect(("127.0.0.1", port))
                print(f" === Port {port} is ready")
                return
        except (socket.error, socket.timeout):
            time.sleep(0.5)
    raise TimeoutError(f"Port {port} did not become available within {timeout} seconds")


class TestContext:
    """Test environment state and operations."""

    def __init__(self, leader_ver: str, follower_ver: str):
        self.leader_ver = leader_ver
        self.follower_ver = follower_ver

        script_dir = Path(__file__).parent
        self.root_dir = script_dir.parent.parent.parent
        os.chdir(self.root_dir)

        self.bins_dir = self.root_dir / "bins"
        self.data_dir = self.root_dir / ".databend"

    def binary(self, version: str, name: str) -> Path:
        return self.bins_dir / version / "bin" / name

    def admin_addr(self, node_id: int) -> str:
        return f"127.0.0.1:{10000 + node_id}"

    def grpc_addr(self, node_id: int) -> str:
        return f"127.0.0.1:{11000 + node_id}"

    def raft_addr(self, node_id: int) -> str:
        return f"127.0.0.1:{12000 + node_id}"

    def start_node(self, version: str, node_id: int, extra_args: list[str], raft_dir: Path = None) -> None:
        """Start databend-meta node."""
        if raft_dir is None:
            raft_dir = self.data_dir / f"meta_{node_id}"

        cmd = [
            str(self.binary(version, "databend-meta")),
            "--id", str(node_id),
            "--admin-api-address", self.admin_addr(node_id),
            "--grpc-api-address", self.grpc_addr(node_id),
            "--raft-listen-host", "127.0.0.1",
            "--raft-advertise-host", "127.0.0.1",
            "--raft-api-port", str(12000 + node_id),
            "--raft-dir", str(raft_dir),
            "--max-applied-log-to-keep", "0",
            "--log-dir", str(self.data_dir / f"meta_log_{node_id}"),
            "--log-stderr-on", "--log-stderr-level", "WARN", "--log-stderr-format", "text",
            "--log-file-on", "--log-file-level", "DEBUG", "--log-file-format", "text",
            *extra_args,
        ]
        print(f" === Running: {' '.join(cmd)}")
        subprocess.Popen(cmd)

        wait_tcp_port(11000 + node_id, timeout=20)
        print(f" === databend-meta ver={version} id={node_id} started")

    def feed_data(self, node_id: int, number: int = 10) -> None:
        """Feed test data to node."""
        cmd = [
            str(self.binary("current", "databend-metabench")),
            "--rpc", 'table_copy_file:{"file_cnt":2,"ttl_ms":86400999}',
            "--client", "1", "--number", str(number), "--prefix", "1",
            "--grpc-api-address", self.grpc_addr(node_id),
        ]
        print(f" === Running: {' '.join(cmd)}")
        subprocess.run(cmd, stdout=subprocess.DEVNULL, check=True)

    def export(self, version: str, source: dict, output: Path) -> None:
        """Export meta data."""
        cmd = [str(self.binary(version, "databend-metactl")), "--export"]
        if "grpc" in source:
            cmd.extend(["--grpc-api-address", source["grpc"]])
        elif "raft_dir" in source:
            cmd.extend(["--raft-dir", str(source["raft_dir"])])

        print(f" === Running: {' '.join(cmd)} > {output}")
        with open(output, "w") as f:
            subprocess.run(cmd, stdout=f, check=True)

    def import_filtered(self, input_file: Path, raft_dir: Path, exclude_pattern: str) -> None:
        """Import data excluding lines matching pattern."""
        cmd = [str(self.binary("current", "databend-metactl")), "--import", "--raft-dir", str(raft_dir)]
        print(f" === Running: {' '.join(cmd)} < {input_file} (filtered)")
        pattern = re.compile(exclude_pattern)
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True)

        with open(input_file, "r") as f:
            for line in f:
                if not pattern.search(line):
                    proc.stdin.write(line)

        proc.stdin.close()
        if proc.wait() != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)

    def filter_and_compare_exports(self, raw1: Path, raw2: Path, include: str = None, exclude: list[str] = None, remove_proposed_at: bool = True, normalize_time_ms: bool = False) -> None:
        """Filter exported data and compare for consistency."""
        filtered1 = raw1.parent / f"{raw1.stem}-filtered"
        filtered2 = raw2.parent / f"{raw2.stem}-filtered"

        transform = None
        if remove_proposed_at or normalize_time_ms:
            def transform(line):
                if remove_proposed_at:
                    proposed_at = re.compile(r',"proposed_at_ms":\d+')
                    line = proposed_at.sub("", line)
                if normalize_time_ms:
                    # Normalize time_ms: truncate last 3 digits
                    time_ms = re.compile(r'"time_ms":(\d+)')
                    def replace_time(match):
                        val = match.group(1)
                        if len(val) >= 3:
                            normalized = val[:-3] + '000'
                        else:
                            normalized = val
                        return f'"time_ms":{normalized}'
                    line = time_ms.sub(replace_time, line)

                    # Normalize expire_at: convert milliseconds to seconds
                    expire_at = re.compile(r'"expire_at":(\d+)')
                    def replace_expire(match):
                        val = match.group(1)
                        if len(val) > 10:
                            # Convert from milliseconds to seconds
                            normalized = val[:-3]
                        else:
                            normalized = val
                        return f'"expire_at":{normalized}'
                    line = expire_at.sub(replace_expire, line)
                return line

        filter_and_sort(raw1, filtered1, include=include, exclude=exclude, transform=transform)
        filter_and_sort(raw2, filtered2, include=include, exclude=exclude, transform=transform)

        print(" === Compare filtered data")
        diff_files(filtered1, filtered2)

    def reimport_and_compare(self, raw1: Path, raw2: Path, prefix: str) -> None:
        """Re-import data with current metactl and verify consistency."""
        print(" === Re-import with current metactl")
        upgrade_dirs = [self.data_dir / "_upgrade_1", self.data_dir / "_upgrade_2"]
        for d in upgrade_dirs:
            d.mkdir(parents=True, exist_ok=True)

        self.import_filtered(raw1, upgrade_dirs[0], '"Expire":\\|"GenericKV":')
        self.import_filtered(raw2, upgrade_dirs[1], '"Expire":\\|"GenericKV":')

        final1_raw = self.data_dir / f"{prefix}-node1-final-raw"
        final2_raw = self.data_dir / f"{prefix}-node2-final-raw"

        self.export("current", {"raft_dir": upgrade_dirs[0]}, final1_raw)
        self.export("current", {"raft_dir": upgrade_dirs[1]}, final2_raw)

        self.filter_and_compare_exports(final1_raw, final2_raw, exclude=["NodeId", "DataHeader"])

    def export_and_compare(self, prefix: str, normalize_time_ms: bool = False) -> tuple[Path, Path]:
        """Export data from nodes 1 and 2 and verify consistency. Returns raw export file paths."""
        print(" === Export and compare state machine data")
        raw1 = self.data_dir / f"{prefix}-node1-raw"
        raw2 = self.data_dir / f"{prefix}-node2-raw"

        self.export(self.leader_ver, {"grpc": self.grpc_addr(1)}, raw1)
        self.export(self.follower_ver, {"grpc": self.grpc_addr(2)}, raw2)

        cmd = ["killall", "databend-meta"]
        print(f" === Running: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        time.sleep(3)

        self.filter_and_compare_exports(raw1, raw2, include="state_machine", exclude=["DataHeader"], normalize_time_ms=normalize_time_ms)

        return raw1, raw2


def get_arch() -> str:
    """Detect architecture for binary downloads."""
    machine = platform.machine().lower()
    system = platform.system().lower()

    arch_map = {
        ("darwin", "x86_64"): "x86_64-apple-darwin",
        ("darwin", "amd64"): "x86_64-apple-darwin",
        ("darwin", "arm64"): "aarch64-apple-darwin",
        ("darwin", "aarch64"): "aarch64-apple-darwin",
        ("linux", "x86_64"): "x86_64-unknown-linux-gnu",
        ("linux", "amd64"): "x86_64-unknown-linux-gnu",
        ("linux", "aarch64"): "aarch64-unknown-linux-gnu",
        ("linux", "arm64"): "aarch64-unknown-linux-gnu",
    }

    arch = arch_map.get((system, machine))
    if not arch:
        raise RuntimeError(f"Unsupported platform: {system} {machine}")
    return arch


def download_binary(ctx: TestContext, version: str) -> None:
    """Download and extract databend binaries."""
    target_dir = ctx.bins_dir / version
    if (target_dir / "bin" / "databend-meta").exists():
        return

    print(f" === Downloading databend version {version}")
    arch = get_arch()
    tarball = f"databend-v{version}-nightly-{arch}.tar.gz"
    url = f"https://github.com/datafuselabs/databend/releases/download/v{version}-nightly/{tarball}"

    target_dir.mkdir(parents=True, exist_ok=True)
    tarball_path = target_dir / tarball
    urllib.request.urlretrieve(url, tarball_path)
    cmd = ["tar", "xzf", tarball]
    print(f" === Running: {' '.join(cmd)} (in {target_dir})")
    subprocess.run(cmd, cwd=target_dir, check=True)
    tarball_path.unlink()


def filter_and_sort(input_file: Path, output_file: Path, include: str = None, exclude: list[str] = None, transform=None) -> None:
    """Generic filter: keep lines matching include, excluding patterns, apply transform, then sort."""
    lines = []

    with open(input_file, "r") as f:
        for line in f:
            if include and include not in line:
                continue
            if exclude and any(pattern in line for pattern in exclude):
                continue
            if transform:
                line = transform(line)
            lines.append(line)

    with open(output_file, "w") as f:
        f.writelines(sorted(lines))


def diff_files(file1: Path, file2: Path) -> None:
    """Compare files and raise if different."""
    print(f" === Running: diff {file1} {file2}")
    result = subprocess.run(["diff", str(file1), str(file2)])
    if result.returncode != 0:
        raise RuntimeError(f"Files differ: {file1} vs {file2}")


def test_snapshot_replication(ctx: TestContext) -> None:
    """Test snapshot replication between different versions."""
    print("\n === Test: Snapshot replication between versions")

    cmd = ["killall", "databend-meta"]
    print(f" === Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=False, stderr=subprocess.DEVNULL)
    if ctx.data_dir.exists():
        shutil.rmtree(ctx.data_dir)
    ctx.data_dir.mkdir(parents=True)

    # Test flow
    print(" === Start leader and feed data")
    ctx.start_node(ctx.leader_ver, 1, ["--single"])
    ctx.feed_data(1)

    print(" === Trigger snapshot and feed more data")
    urllib.request.urlopen(f"http://{ctx.admin_addr(1)}/v1/ctrl/trigger_snapshot").read()
    time.sleep(3)
    ctx.feed_data(1)

    print(" === Start follower")
    ctx.start_node(ctx.follower_ver, 2, ["--join", ctx.raft_addr(1)])
    time.sleep(3)

    raw1, raw2 = ctx.export_and_compare("snapshot")
    ctx.reimport_and_compare(raw1, raw2, "snapshot")

    print(" === Snapshot replication test completed successfully!")


def test_vote_request(ctx: TestContext) -> None:
    """Test vote request between different versions."""
    print("\n === Test: Vote request between versions")

    cmd = ["killall", "databend-meta"]
    print(f" === Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=False, stderr=subprocess.DEVNULL)
    time.sleep(3)
    if ctx.data_dir.exists():
        shutil.rmtree(ctx.data_dir)
    ctx.data_dir.mkdir(parents=True)

    # Create 2-node cluster configuration
    vote_dirs = [ctx.data_dir / f"vote_{i}" for i in [1, 2]]
    for d in vote_dirs:
        d.mkdir(parents=True, exist_ok=True)

    print(f" === Build up cluster of two nodes")

    # Start node 1 with leader version
    print(f" === Start node 1 with version {ctx.leader_ver}")
    ctx.start_node(ctx.leader_ver, 1, ["--single"], vote_dirs[0])

    # Start node 2 with follower version - this will trigger vote request
    print(f" === Start node 2 with version {ctx.follower_ver}")
    ctx.start_node(ctx.follower_ver, 2, ["--join", ctx.raft_addr(1)], vote_dirs[1])

    # Wait for cluster to stabilize
    time.sleep(3)

    # Feed data to node 1 (leader)
    print(" === Feed data to node 1")
    ctx.feed_data(1, number=5)


    cmd = ["killall", "databend-meta"]
    print(f" === Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=False, stderr=subprocess.DEVNULL)
    time.sleep(3)


    print(f" === Restart cluster of two nodes")

    print(f" === Restart node 1 with version {ctx.leader_ver}")
    ctx.start_node(ctx.leader_ver, 1, ["--single"], vote_dirs[0])

    print(f" === Restart node 2 with version {ctx.follower_ver}")
    ctx.start_node(ctx.follower_ver, 2, ["--join", ctx.raft_addr(1)], vote_dirs[1])

    time.sleep(3)

    print(" === Feed mroe data to node 1")
    ctx.feed_data(1, number=5)
    time.sleep(1)


    # Trigger snapshot on both nodes
    print(" === Trigger snapshot on both nodes")
    urllib.request.urlopen(f"http://{ctx.admin_addr(1)}/v1/ctrl/trigger_snapshot").read()
    urllib.request.urlopen(f"http://{ctx.admin_addr(2)}/v1/ctrl/trigger_snapshot").read()
    time.sleep(3)

    ctx.export_and_compare("vote", normalize_time_ms=True)

    print(" === Vote request test completed successfully!")


def main():
    parser = argparse.ArgumentParser(description="Test databend-meta compatibility between versions")
    parser.add_argument("leader_version", help="Leader version (e.g., '0.7.151' or 'current')")
    parser.add_argument("follower_version", help="Follower version (e.g., '0.7.151' or 'current')")
    args = parser.parse_args()

    ctx = TestContext(args.leader_version, args.follower_version)
    os.environ["RUST_BACKTRACE"] = "full"

    print(f" === ROOT: {ctx.root_dir}")
    print(f" === Leader: {ctx.leader_ver}, Follower: {ctx.follower_ver}")

    # Setup
    for binary in (ctx.bins_dir / "current" / "bin").iterdir():
        if binary.is_file():
            binary.chmod(0o755)

    for version in [ctx.leader_ver, ctx.follower_ver]:
        if version != "current":
            download_binary(ctx, version)

    # Run tests
    test_snapshot_replication(ctx)
    test_vote_request(ctx)

    print(" === All tests completed successfully!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n === Interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n === Failed: {e}")
        sys.exit(1)
