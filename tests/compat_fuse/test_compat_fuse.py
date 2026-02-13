#!/usr/bin/env python3
"""
Fuse table format compatibility test.

Tests both backward (old writer → current reader) and forward (current writer → old reader)
compatibility across query versions with meta upgrades.
"""

import argparse
import os
import platform
import shutil
import socket
import subprocess
import sys
import tarfile
import time
import urllib.request
from pathlib import Path


def load_test_cases(path: Path) -> list[dict]:
    """Load test cases from a YAML file."""
    import yaml

    with open(path) as f:
        cases = yaml.safe_load(f)
    # Ensure all values in meta lists are strings (yaml may parse "1.2.527" as float)
    for case in cases:
        case["meta"] = [str(v) for v in case["meta"]]
        if "writer" in case:
            case["writer"] = str(case["writer"])
        if "reader" in case:
            case["reader"] = str(case["reader"])
        if "suite" in case:
            case["suite"] = str(case["suite"])
    return cases


def wait_tcp_port(port: int, timeout: int = 20) -> None:
    """Wait for TCP port to become available on 127.0.0.1."""
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


def get_arch() -> str:
    """Detect architecture for binary downloads."""
    machine = platform.machine().lower()
    arch_map = {
        "x86_64": "x86_64-unknown-linux-gnu",
        "amd64": "x86_64-unknown-linux-gnu",
        "aarch64": "aarch64-unknown-linux-gnu",
        "arm64": "aarch64-unknown-linux-gnu",
    }
    arch = arch_map.get(machine)
    if not arch:
        raise RuntimeError(f"Unsupported architecture: {machine}")
    return arch


def kill_proc(name: str) -> None:
    """Kill all processes matching name, force-kill if still alive."""
    print(f" === Kill {name} ...")

    result = subprocess.run(["killall", name], check=False)
    if result.returncode != 0:
        print(f" === No {name} to kill")
        return

    time.sleep(1)

    result = subprocess.run(["pgrep", name], check=False, capture_output=True)
    if result.returncode == 0:
        print(f" === {name} still alive, force killing")
        subprocess.run(["killall", "-9", name], check=False)

    print(f" === Done kill {name}")


def query_rel_config_path() -> str:
    return "scripts/ci/deploy/config/databend-query-node-1.toml"


def git_partial_clone(
    repo_url: str, branch: str, path: str, repo_local_dir: str
) -> None:
    """Clone only specified dir or file at the specified ref.

    Args:
        repo_url: URL of the git repository
        branch: Branch or tag to clone
        path: Path within the repository to check out
        repo_local_dir: Local path to clone into
    """
    print(f" === Clone {repo_url}@{branch}:{path}")
    print(f" ===    To {repo_local_dir}/{path}")

    if Path(repo_local_dir).exists():
        shutil.rmtree(repo_local_dir)
        print(f" === Removed existing {repo_local_dir}")

    subprocess.run(
        [
            "git",
            "-c", "advice.detachedHead=false",
            "clone",
            "-b",
            branch,
            "--depth",
            "1",
            "--quiet",
            "--filter=blob:none",
            "--sparse",
            repo_url,
            repo_local_dir,
        ],
        check=True,
    )

    print(f" === Checkout: {path}")
    subprocess.run(
        ["git", "sparse-checkout", "set", path], check=True, cwd=repo_local_dir
    )

    print(f" === Done clone from {repo_url}@{branch}:{path}")
    print(" === Cloned files:")
    for f in Path(repo_local_dir, path).glob("*"):
        print(f" ===     {f}")


class TestContext:
    """Test environment state and operations for fuse compatibility testing."""

    def __init__(
        self,
        writer_ver: str,
        reader_ver: str,
        meta_versions: list[str],
        logictest_path: str,
    ):
        self.writer_ver = writer_ver
        self.reader_ver = reader_ver
        self.meta_versions = meta_versions
        self.logictest_path = logictest_path
        self.processes: list[subprocess.Popen] = []

        self.root_dir = Path.cwd()
        self.bins_dir = self.root_dir / "bins"
        self.data_dir = self.root_dir / ".databend"

    def bin_path(self, ver: str, bin_name: str) -> str:
        return str(self.bins_dir / ver / "bin" / f"databend-{bin_name}")

    def query_config_path(self, ver: str) -> str:
        relative_path = query_rel_config_path()
        if ver == "current":
            return relative_path
        return f"v{ver}_config/{relative_path}"

    def cleanup(self) -> None:
        """Terminate all tracked processes, then killall stragglers."""
        for proc in self.processes:
            if proc.poll() is None:
                proc.terminate()
        for proc in self.processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        self.processes.clear()

        kill_proc("databend-query")
        kill_proc("databend-meta")
        time.sleep(1)

    def start_metasrv(self, ver: str) -> subprocess.Popen:
        """Start databend-meta, track process, wait for port 9191."""
        metasrv_path = self.bin_path(ver, "meta")
        stdout_file = str(self.data_dir / f"meta-nohup-{ver}.out")

        print(f" === Start {metasrv_path} databend-meta...")

        f = open(stdout_file, "w")
        proc = subprocess.Popen(
            [metasrv_path, "--single", "--log-level=DEBUG"], stdout=f, stderr=f
        )
        self.processes.append(proc)

        try:
            wait_tcp_port(9191, 20)
        except TimeoutError:
            proc.terminate()
            proc.wait()
            f.close()
            with open(stdout_file, "r") as log:
                print(log.read())
            raise RuntimeError(f"Failed to start {metasrv_path}")

        return proc

    def start_query(self, ver: str) -> subprocess.Popen:
        """Start databend-query, track process, wait for port 3307."""
        query_path = self.bin_path(ver, "query")
        config_path = self.query_config_path(ver)
        stdout_file = str(self.data_dir / f"query-nohup-{ver}.out")

        print(f" === Start {query_path} databend-query... config: {config_path}")

        f = open(stdout_file, "w")
        proc = subprocess.Popen(
            [
                query_path,
                "-c",
                config_path,
                "--log-level",
                "DEBUG",
                "--meta-endpoints",
                "0.0.0.0:9191",
            ],
            stdout=f,
            stderr=f,
        )
        self.processes.append(proc)

        try:
            wait_tcp_port(3307, 20)
        except TimeoutError:
            proc.terminate()
            proc.wait()
            f.close()
            with open(stdout_file, "r") as log:
                print(log.read())
            raise RuntimeError(f"Failed to start {query_path} with {config_path}")

        return proc

    def run_sqllogictests(self, run_file: str) -> None:
        """Run sqllogictests for the given test file."""
        print(f" === Run test: {run_file}")

        sqllogictests = str(self.bins_dir / "current" / "bin" / "databend-sqllogictests")
        subprocess.run(
            [
                sqllogictests,
                "--handlers",
                "mysql",
                "--suites",
                f"tests/compat_fuse/compat-logictest/{self.logictest_path}",
                "--run_file",
                run_file,
            ],
            check=True,
        )

    def _print_bin_version(self, bin_path: str, args: list[str]) -> None:
        """Try to print binary version info; suppress noise from old binaries."""
        result = subprocess.run(
            [bin_path] + args,
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                if line.startswith("version:"):
                    print(f"        {line}")
                    return
            print(f"        (no version line in output)")
        else:
            print(f"        (--cmd ver not supported by this build)")

    def clean_data_dir(self) -> None:
        """Remove and recreate the .databend data directory."""
        print(" === Clean data dir")
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def run_test(self) -> None:
        """Run the full write -> meta-upgrade -> read compatibility test."""
        self.cleanup()
        self.clean_data_dir()

        try:
            # Verify downloaded binaries by printing their versions
            print(" === Checking binary versions before test ...")

            for ver in self.meta_versions:
                print(f" === checking metasrv {ver}")
                self._print_bin_version(self.bin_path(ver, "meta"), ["--single", "--cmd", "ver"])

            print(f" === checking writer query {self.writer_ver}")
            self._print_bin_version(self.bin_path(self.writer_ver, "query"), ["--cmd", "ver"])

            print(f" === checking reader query {self.reader_ver}")
            self._print_bin_version(self.bin_path(self.reader_ver, "query"), ["--cmd", "ver"])

            print(" === All binaries OK")

            # Phase 1: Write
            print(" === Phase 1: Write data with writer version")
            self.start_metasrv(self.meta_versions[0])
            self.start_query(self.writer_ver)
            self.run_sqllogictests("fuse_compat_write.test")
            self.cleanup()

            # Phase 2: Meta upgrade
            print(" === Phase 2: Upgrade meta on-disk data")
            for ver in self.meta_versions:
                self.start_metasrv(ver)
                time.sleep(3)
                self.cleanup()

            # Phase 3: Read
            print(" === Phase 3: Read data with reader version")
            self.start_metasrv(self.meta_versions[-1])
            self.start_query(self.reader_ver)
            self.run_sqllogictests("fuse_compat_read.test")
        finally:
            self.cleanup()


def download_query_config(version: str) -> None:
    """Download config.toml for a specific version of query.

    Args:
        version: Version string without prefix 'v' or '-nightly'
    """
    if version == "current":
        print(" === Skip downloading config for current version")
        return

    bend_repo_url = "https://github.com/datafuselabs/databend"
    query_config_dir = str(Path(query_rel_config_path()).parent)
    repo_local_dir = f"v{version}_config"

    print(f" === Download query config.toml from {version}:{query_config_dir}")

    git_partial_clone(
        bend_repo_url, f"v{version}-nightly", query_config_dir, repo_local_dir
    )


def download_binary(version: str) -> None:
    """Download and extract databend binaries for the given version."""
    if version == "current":
        print(" === Skip downloading binary for current version")
        return

    arch = get_arch()
    url = f"https://github.com/datafuselabs/databend/releases/download/v{version}-nightly/databend-v{version}-nightly-{arch}.tar.gz"
    fn = f"databend-{version}.tar.gz"
    bin_dir = Path(f"./bins/{version}")

    # Check if binaries already exist
    if (bin_dir / "bin" / "databend-query").exists():
        print(f" === Binaries exist: {' '.join(str(p) for p in bin_dir.glob('*'))}")
        for binary in bin_dir.glob("*"):
            binary.chmod(0o755)
        return

    # Download if tar file doesn't exist
    if Path(fn).exists():
        print(f" === Tar file exists: {fn}")
    else:
        print(f" === Download binary ver: {version}")
        print(f" === Download binary url: {url}")

        max_retries = 5
        for attempt in range(max_retries):
            try:
                urllib.request.urlretrieve(url, fn)
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                print(f" === Download attempt {attempt + 1} failed: {e}")
                time.sleep(1)

    # Create directory and extract
    bin_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fn, "r:gz") as tar:
        tar.extractall(path=bin_dir, filter="data")

    print(f" === Unpacked: {bin_dir}:")
    print(f" === {' '.join(str(p) for p in bin_dir.glob('*'))}")

    # Make binaries executable
    for binary in bin_dir.glob("*"):
        binary.chmod(0o755)


def setup_env() -> None:
    """Common environment setup: cd to repo root, set env, chmod binaries."""
    script_dir = Path(__file__).resolve().parent
    os.chdir(script_dir / ".." / "..")

    os.environ["RUST_BACKTRACE"] = "full"

    current_bin_dir = Path("./bins/current/bin")
    if current_bin_dir.exists():
        for binary in current_bin_dir.iterdir():
            if binary.is_file():
                binary.chmod(0o755)

    Path(".databend").mkdir(parents=True, exist_ok=True)


def download_and_run_case(
    writer: str, reader: str, meta: list[str], suite: str,
) -> None:
    """Download binaries/configs and run one compatibility test case."""
    download_query_config(writer)
    download_query_config(reader)

    for meta_ver in meta:
        download_binary(meta_ver)

    download_binary(writer)
    download_binary(reader)

    ctx = TestContext(writer, reader, meta, suite)
    ctx.run_test()


def main():
    parser = argparse.ArgumentParser(
        description="Assert that latest query is compatible with an old version query on fuse-table format"
    )
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="Run all test cases defined in test_cases.yaml",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print test cases without executing (use with --run-all)",
    )
    parser.add_argument(
        "--writer-version",
        help="The version that writes data to test compatibility with",
    )
    parser.add_argument(
        "--reader-version",
        help="The version that reads data from test compatibility with",
    )
    parser.add_argument(
        "--meta-versions",
        nargs="+",
        help="The databend-meta version(s) to run; the first one will be used as the writer's meta, the last one will be used as the reader's meta. The others used to upgrade ondisk meta data",
    )
    parser.add_argument(
        "--logictest-path",
        default="./base",
        help="Default sqllogic test suite is 'tests/compat_fuse/compat-logictest/'",
    )

    args = parser.parse_args()

    if args.run_all:
        setup_env()

        yaml_path = Path(__file__).resolve().parent / "test_cases.yaml"
        cases = load_test_cases(yaml_path)
        print(f" === Loaded {len(cases)} test cases from {yaml_path}")

        for i, case in enumerate(cases):
            writer = case["writer"]
            reader = case["reader"]
            meta = case["meta"]
            suite = case["suite"]
            print(f" === [{i+1}/{len(cases)}] writer={writer} reader={reader} meta={meta} suite={suite}")

            if args.dry_run:
                continue

            download_and_run_case(writer, reader, meta, suite)

        if args.dry_run:
            print(" === Dry run complete, no tests executed.")
        else:
            print(" === All tests completed successfully!")
    else:
        if not args.writer_version or not args.reader_version or not args.meta_versions:
            parser.error(
                "--writer-version, --reader-version, and --meta-versions are required "
                "when not using --run-all"
            )

        print(f" === args: {args}")
        setup_env()
        download_and_run_case(
            args.writer_version,
            args.reader_version,
            args.meta_versions,
            args.logictest_path,
        )
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
