#!/usr/bin/env python3

import socket
import os
import sys
import subprocess
from pathlib import Path
from typing import List
import shutil
import argparse
import urllib.request
import tarfile
import time
import platform


def dd(*args):
    print(" === ", *args)


def get_arch() -> str:
    """Detect current architecture and return the appropriate target string."""
    arch = platform.machine()
    if arch == "x86_64":
        return "x86_64-unknown-linux-gnu"
    elif arch in ["aarch64", "arm64"]:
        return "aarch64-unknown-linux-gnu"
    else:
        raise ValueError(f"Unsupported architecture: {arch}")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments and return the args object.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Assert that latest query being compatible with an old version query on fuse-table format"
    )
    parser.add_argument(
        "--writer-version",
        required=True,
        help="The version that writes data to test compatibility with",
    )
    parser.add_argument(
        "--reader-version",
        required=True,
        help="The version that reads data from test compatibility with",
    )
    parser.add_argument(
        "--meta-versions",
        required=True,
        nargs="+",
        help="The databend-meta version(s) to run; the first one will be used as the writer's meta, the last one will be used as the reader's meta. The others used to upgrade ondisk meta data",
    )
    parser.add_argument(
        "--logictest-path",
        default="./base",
        help="Default sqllogic test suite is 'tests/compat_fuse/compat-logictest/'",
    )

    args = parser.parse_args()
    dd(f"args: {args}")

    return args


def download_query_config(version: str) -> None:
    """Download config.toml for a specific version of query.

    Args:
        version: Version string without prefix 'v' or '-nightly'
        repo_local_dir: Local directory to store the config
    """

    if version == "current":
        dd("Skip downloading config for current version")
        return

    bend_repo_url = "https://github.com/datafuselabs/databend"
    query_config_dir = str(Path(query_rel_config_path()).parent)
    repo_local_dir = f"v{version}_config"

    dd(f"Download query config.toml from {version}:{query_config_dir}")

    git_partial_clone(
        bend_repo_url, f"v{version}-nightly", query_config_dir, repo_local_dir
    )


def binary_url(ver: str) -> str:
    arch = get_arch()
    return f"https://github.com/datafuselabs/databend/releases/download/v{ver}-nightly/databend-v{ver}-nightly-{arch}.tar.gz"


def bin_path(ver: str, bin_name: str) -> str:
    return f"./bins/{ver}/bin/databend-{bin_name}"


def query_config_path(ver: str) -> str:
    relative_path = query_rel_config_path()

    if ver == "current":
        return relative_path
    else:
        return f"v{ver}_config/{relative_path}"


def query_rel_config_path():
    relative_path = "scripts/ci/deploy/config/databend-query-node-1.toml"
    return relative_path


def download_binary(version: str) -> None:
    if version == "current":
        dd("Skip downloading binary for current version")
        return

    url = binary_url(version)
    fn = f"databend-{version}.tar.gz"
    bin_dir = Path(f"./bins/{version}")

    dd("Expect tar file:", Path(fn))

    # Check if binaries already exist
    if (bin_dir / "bin" / "databend-query").exists():
        dd(f"Binaries exist: {' '.join(str(p) for p in bin_dir.glob('*'))}")
        for binary in bin_dir.glob("*"):
            binary.chmod(0o755)
        return

    # Download if tar file doesn't exist
    if Path(fn).exists():
        dd(f"Tar file exists: {fn}")
    else:
        dd(f"Download binary ver: {version}")
        dd(f"Download binary url: {url}")

        # Download with retries
        max_retries = 5
        for attempt in range(max_retries):
            try:
                urllib.request.urlretrieve(url, fn)
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                dd(f"Download attempt {attempt + 1} failed: {e}")
                import time

                time.sleep(1)

    # Create directory and extract
    bin_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fn, "r:gz") as tar:
        tar.extractall(path=bin_dir)

    dd(f"Unpacked: {bin_dir}:")
    dd(" ".join(str(p) for p in bin_dir.glob("*")))

    # Make binaries executable
    for binary in bin_dir.glob("*"):
        binary.chmod(0o755)


def run_test(
    writer_ver: str, reader_ver: str, meta_versions: List[str], logictest_path: str
) -> None:
    """Test fuse-data compatibility between an old version query and the current version query.

    Args:
        writer_ver: Writer query version
        reader_ver: Reader query version
        meta_versions: Meta versions
        logictest_path: Path to logic test suite
    """
    dd(f"Test with query-{writer_ver} and current query")

    for ver in meta_versions:
        dd("metasrv for writer query version")
        try:
            subprocess.run(
                [bin_path(ver, "meta"), "--single", "--cmd", "ver"], check=True
            )
        except subprocess.CalledProcessError:
            dd("no version yet")

    dd("writer query version:")
    try:
        subprocess.run([bin_path(writer_ver, "query"), "--cmd", "ver"], check=True)
    except subprocess.CalledProcessError:
        dd("no version yet")

    dd("reader query version:")
    try:
        subprocess.run([bin_path(reader_ver, "query"), "--cmd", "ver"], check=True)
    except subprocess.CalledProcessError:
        dd("no version yet")

    time.sleep(1)

    kill_proc("databend-query")
    kill_proc("databend-meta")

    dd("Clean meta dir")
    try:
        shutil.rmtree(".databend")
    except FileNotFoundError:
        dd("no dir to rm: .databend")

    try:
        os.remove("nohup.out")
    except FileNotFoundError:
        dd("no nohup.out")

    Path(".databend").mkdir(parents=True, exist_ok=True)

    meta_proc = start_metasrv(meta_versions[0])
    query_proc = start_query(writer_ver)
    run_sqllogictests(logictest_path, "fuse_compat_write")

    meta_proc.terminate()
    query_proc.terminate()
    time.sleep(1)

    kill_proc("databend-query")
    kill_proc("databend-meta")

    # upgrade meta ondisk data
    for ver in meta_versions:
        meta_proc = start_metasrv(ver)
        time.sleep(3)
        meta_proc.terminate()

        kill_proc("databend-meta")

    meta_proc = start_metasrv(meta_versions[-1])
    query_proc = start_query(reader_ver)
    run_sqllogictests(logictest_path, "fuse_compat_read")

    meta_proc.terminate()
    query_proc.terminate()

    kill_proc("databend-query")
    kill_proc("databend-meta")

    # sleep a short while to ensure the processes are terminated
    time.sleep(1)


def git_partial_clone(
    repo_url: str, branch: str, path: str, repo_local_dir: str
) -> None:
    """Clone only specified dir or file in the specified commit.

    Args:
        repo_url: URL of the git repository
        branch: Branch or tag to clone
        path: Path to clone in the repository
        repo_local_dir: Local path to clone to
    """
    dd(f"Clone {repo_url}@{branch}:{path}")
    dd(f"   To {repo_local_dir}/{path}")

    # Remove existing directory if it exists
    if Path(repo_local_dir).exists():
        shutil.rmtree(repo_local_dir)
        dd(f"Removed existing {repo_local_dir}")

    # Clone repository
    subprocess.run(
        [
            "git",
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

    dd(f"Checkout: {path}")

    subprocess.run(
        ["git", "sparse-checkout", "set", path], check=True, cwd=repo_local_dir
    )

    dd(f"Done clone from {repo_url}@{branch}:{path}")
    dd("Cloned files:")
    for f in Path(repo_local_dir, path).glob("*"):
        dd(f"    {f}")


def kill_proc(name: str) -> None:
    dd(f"Kill {name} ...")

    try:
        subprocess.run(["killall", name], check=False)
    except subprocess.CalledProcessError:
        dd(f"no {name} to kill")
        return

    time.sleep(1)

    # Check if process is still running
    try:
        subprocess.run(["pgrep", name], check=True, capture_output=True)
        dd(f"The {name} is not killed. force killing.")
        subprocess.run(["killall", "-9", name], check=False)
    except subprocess.CalledProcessError:
        dd(f"no {name} to killall-9")

    dd(f"Done kill {name}")


def start_metasrv(ver: str) -> subprocess.Popen:
    metasrv_path = bin_path(ver, "meta")
    stdout_file = f".databend/meta-nohup-{ver}.out"

    dd(f"Start {metasrv_path} databend-meta...")

    with open(stdout_file, "w") as f:
        meta_process = subprocess.Popen(
            [metasrv_path, "--single", "--log-level=DEBUG"], stdout=f, stderr=f
        )

    if not tcp_ping(9191, 20):
        dd("Fail to connect to :{}".format(9191))

        # shutdown the query process, output stdout and stderr
        meta_process.terminate()
        meta_process.wait()
        with open(stdout_file, "r") as f:
            print(f.read())

        raise Exception(f"Fail to start {metasrv_path}")

    return meta_process


def start_query(ver: str) -> subprocess.Popen:
    query_path = bin_path(ver, "query")
    config_path = query_config_path(ver)
    stdout_file = f".databend/query-nohup-{ver}.out"

    dd(f"Start {query_path} databend-query... config: {config_path}")

    with open(stdout_file, "w") as f:
        query_process = subprocess.Popen(
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

    if not tcp_ping(3307, 20):
        dd("Fail to connect to :{}".format(3307))

        # shutdown the query process, output stdout and stderr
        query_process.terminate()
        query_process.wait()
        with open(stdout_file, "r") as f:
            print(f.read())

        raise Exception(f"Fail to start {query_path} with {config_path}")

    return query_process


def run_sqllogictests(logictest_path: str, run_file: str) -> None:
    dd(f"Run test: {run_file}")

    sqllogictests = "./bins/current/bin/databend-sqllogictests"

    subprocess.run(
        [
            sqllogictests,
            "--handlers",
            "mysql",
            "--suites",
            f"tests/compat_fuse/compat-logictest/{logictest_path}",
            "--run_file",
            run_file,
        ],
        check=True,
    )


def tcp_ping(port, timeout) -> bool:
    now = time.time()

    while time.time() - now < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(("0.0.0.0", port))
                dd(f"OK :{port} is listening")
                sys.stdout.flush()
                return True
        except Exception:
            dd(f"... connecting to :{port}")
            sys.stdout.flush()
            time.sleep(1)

    return False


def main():
    args = parse_args()

    download_query_config(args.writer_version)
    download_query_config(args.reader_version)

    for meta_ver in args.meta_versions:
        download_binary(meta_ver)

    download_binary(args.writer_version)

    run_test(
        args.writer_version,
        args.reader_version,
        args.meta_versions,
        args.logictest_path,
    )


if __name__ == "__main__":
    main()
