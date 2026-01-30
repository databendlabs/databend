import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import zipfile
from pathlib import Path
from urllib.parse import urlparse

try:
    import requests
except ImportError as exc:
    raise SystemExit("requests is required; run via nox session") from exc


ROOT = Path(__file__).resolve().parents[2]


class SandboxContext:
    def __init__(self) -> None:
        self.base_url = os.environ.get("DATABEND_HTTP_URL", "http://localhost:8000")
        self.s3_endpoint_url = os.environ.get("S3_ENDPOINT_URL", "http://127.0.0.1:9900")
        self.s3_access_key = os.environ.get("S3_ACCESS_KEY_ID", "minioadmin")
        self.s3_secret_key = os.environ.get("S3_SECRET_ACCESS_KEY", "minioadmin")
        self.udf_import_presign_expire_secs = os.environ.get(
            "UDF_IMPORT_PRESIGN_EXPIRE_SECS", "259200"
        )
        self.udf_docker_base_image = os.environ.get("UDF_DOCKER_BASE_IMAGE", "python:3.12-slim")
        self.udf_docker_runtime_image = os.environ.get(
            "UDF_DOCKER_RUNTIME_IMAGE", "databend-udf-runtime:py312"
        )
        self.udf_query_timeout_secs = int(os.environ.get("UDF_QUERY_TIMEOUT_SECS", "180"))
        self.udf_import_stage = os.environ.get("UDF_IMPORT_STAGE", "udf_import_stage")
        self.build_profile = os.environ.get("BUILD_PROFILE", "debug")
        self._settings = self._load_settings()

    def _load_settings(self):
        settings_raw = os.environ.get("UDF_SETTINGS_JSON", "").strip()
        if settings_raw:
            return json.loads(settings_raw)
        return {"udf_cloud_import_presign_expire_secs": str(self.udf_import_presign_expire_secs)}

    def request_json(self, method: str, url: str, payload=None):
        auth = ("root", "")
        response = requests.request(method, url, json=payload, auth=auth, timeout=30)
        try:
            data = response.json()
        except json.JSONDecodeError:
            raise RuntimeError(f"invalid json response: {response.text}") from None
        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code}: {data}")
        return data

    def check_response_error(self, response):
        if response.get("state") == "Failed":
            error = response.get("error") or {}
            message = error.get("message") if isinstance(error, dict) else str(error)
            raise RuntimeError(f"[Test Error] {message}")

    def execute_query(self, sql: str):
        payload = {"sql": sql}
        if self._settings is not None:
            payload["settings"] = self._settings
        response = self.request_json("POST", f"{self.base_url}/v1/query", payload)
        self.check_response_error(response)
        return response

    def collect_query_data(self, response):
        data = response.get("data") or []
        next_uri = response.get("next_uri") or ""
        start_ts = time.time()
        while next_uri:
            if time.time() - start_ts >= self.udf_query_timeout_secs:
                raise RuntimeError(f"[Test Error] query timed out after {self.udf_query_timeout_secs}s")
            response = self.request_json("GET", f"{self.base_url}{next_uri}")
            self.check_response_error(response)
            page = response.get("data") or []
            data.extend(page)
            next_uri = response.get("next_uri") or ""
            if next_uri and not page:
                time.sleep(0.2)
        return data

    def check_result(self, sql: str, expected):
        response = self.execute_query(sql)
        actual = self.collect_query_data(response)
        if actual != expected:
            raise RuntimeError(f"Mismatch\nExpected: {expected}\nActual  : {actual}")

    def execute_query_expect_error(self, sql: str, expected_substring: str):
        payload = {"sql": sql}
        if self._settings is not None:
            payload["settings"] = self._settings
        response = self.request_json("POST", f"{self.base_url}/v1/query", payload)
        error = response.get("error")
        if not error:
            raise RuntimeError(f"[Test Error] expected query failure but got: {response}")
        message = error.get("message") if isinstance(error, dict) else str(error)
        if expected_substring and expected_substring not in message:
            raise RuntimeError(
                f"[Test Error] expected error containing '{expected_substring}' but got: {message}"
            )

    def upload_stage_file(self, stage_name: str, relative_path: str, file_path: Path):
        headers = {
            "x-databend-stage-name": stage_name,
            "x-databend-relative-path": relative_path,
        }
        with file_path.open("rb") as handle:
            response = requests.put(
                f"{self.base_url}/v1/upload_to_stage",
                headers=headers,
                files={"file": handle},
                auth=("root", ""),
                timeout=30,
            )
        data = response.json()
        if data.get("state") != "SUCCESS":
            raise RuntimeError(f"[Test Error] upload to stage failed: {data}")


def run_command(command, *, env=None, check=True):
    return subprocess.run(command, cwd=ROOT, env=env, check=check)


def run_command_optional(command):
    try:
        subprocess.run(command, cwd=ROOT, check=False)
    except FileNotFoundError:
        pass


def require_command(cmd):
    if shutil.which(cmd) is None:
        raise RuntimeError(f"{cmd} is required for Sandbox UDF test")


def docker_image_exists(image: str) -> bool:
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def wait_port(host: str, port: int, timeout: int):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"[Test Error] timeout waiting for {host}:{port}")


def apply_query_settings(config_path: Path):
    lines = config_path.read_text().splitlines()
    output = []
    in_query = False
    added = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            if in_query and not added:
                output.append("enable_udf_sandbox = true")
                output.append('cloud_control_grpc_server_address = "http://0.0.0.0:50051"')
                added = True
            in_query = stripped == "[query]"
            output.append(line)
            continue
        if in_query and stripped.startswith("enable_udf_sandbox"):
            continue
        if in_query and stripped.startswith("cloud_control_grpc_server_address"):
            continue
        output.append(line)
    if not added:
        if not in_query:
            output.append("[query]")
        output.append("enable_udf_sandbox = true")
        output.append('cloud_control_grpc_server_address = "http://0.0.0.0:50051"')
    config_path.write_text("\n".join(output) + "\n")


def start_process(command, output_path: Path, env=None):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    handle = output_path.open("w")
    return subprocess.Popen(command, cwd=ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT)


def prepare_imports(ctx: SandboxContext):
    print("Preparing stage imports")
    parsed = urlparse(ctx.s3_endpoint_url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    wait_port(host, port, 30)

    create_stage_sql = f"""CREATE STAGE IF NOT EXISTS {ctx.udf_import_stage}
URL = 's3://testbucket/udf-imports/'
CONNECTION = (
  access_key_id = '{ctx.s3_access_key}',
  secret_access_key = '{ctx.s3_secret_key}',
  endpoint_url = '{ctx.s3_endpoint_url}'
);
"""
    ctx.execute_query(create_stage_sql)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        helper_file = tmp_path / "helper.py"
        data_file = tmp_path / "data.txt"
        extra_file = tmp_path / "extra.txt"
        dup_a_dir = tmp_path / "dup_a"
        dup_b_dir = tmp_path / "dup_b"
        dup_a_file = dup_a_dir / "dup.txt"
        dup_b_file = dup_b_dir / "dup.txt"
        archive_inner = tmp_path / "archive.txt"
        archive_file = tmp_path / "archive.tar.gz"
        zip_mod = tmp_path / "zip_mod.py"
        whl_mod = tmp_path / "whl_mod.py"
        egg_mod = tmp_path / "egg_mod.py"
        zip_file = tmp_path / "lib.zip"
        whl_file = tmp_path / "lib.whl"
        egg_file = tmp_path / "lib.egg"

        helper_file.write_text("def add_one(x: int) -> int:\n    return x + 1\n")
        data_file.write_text("stage_data\n")
        extra_file.write_text("extra_data\n")
        dup_a_dir.mkdir(parents=True, exist_ok=True)
        dup_b_dir.mkdir(parents=True, exist_ok=True)
        dup_a_file.write_text("dup_a\n")
        dup_b_file.write_text("dup_b\n")
        archive_inner.write_text("archive_data\n")
        zip_mod.write_text('VALUE = "zip"\n')
        whl_mod.write_text('VALUE = "whl"\n')
        egg_mod.write_text('VALUE = "egg"\n')

        with tarfile.open(archive_file, "w:gz") as tf:
            tf.add(archive_inner, arcname="archive.txt")

        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.write(zip_mod, arcname="zip_mod.py")
        with zipfile.ZipFile(whl_file, "w") as zf:
            zf.write(whl_mod, arcname="whl_mod.py")
        with zipfile.ZipFile(egg_file, "w") as zf:
            zf.write(egg_mod, arcname="egg_mod.py")

        ctx.upload_stage_file(ctx.udf_import_stage, "imports", helper_file)
        ctx.upload_stage_file(ctx.udf_import_stage, "imports", data_file)
        ctx.upload_stage_file(ctx.udf_import_stage, "imports", archive_file)
        ctx.upload_stage_file(ctx.udf_import_stage, "imports/subdir", extra_file)
        ctx.upload_stage_file(ctx.udf_import_stage, "imports/dup_a", dup_a_file)
        ctx.upload_stage_file(ctx.udf_import_stage, "imports/dup_b", dup_b_file)
        ctx.upload_stage_file(ctx.udf_import_stage, "imports", zip_file)
        ctx.upload_stage_file(ctx.udf_import_stage, "imports", whl_file)
        ctx.upload_stage_file(ctx.udf_import_stage, "imports", egg_file)


def metadata_imports(ctx: SandboxContext):
    print("Creating python UDF")
    create_udf_sql = f"""CREATE OR REPLACE FUNCTION add_one(INT)
RETURNS INT
LANGUAGE PYTHON
IMPORTS = ('@{ctx.udf_import_stage}/imports/helper.py')
PACKAGES = ()
HANDLER = 'add_one'
AS $$
# This metadata dependency is required for sandbox UDFs too; without parsing it
# the cloud path misses the package and raises ModuleNotFoundError. TOML format
# /// script
# dependencies = ["humanize==4.9.0"]
# ///
import helper
import humanize

def add_one(x: int) -> int:
    humanize.intcomma(x)
    return helper.add_one(x)
$$"""
    ctx.execute_query(create_udf_sql)

    print("Executing metadata imports UDF")
    ctx.check_result("SELECT add_one(1) AS result", [["2"]])


def numpy_case(ctx: SandboxContext):
    print("Creating numpy UDF")
    create_numpy_udf_sql = """CREATE OR REPLACE FUNCTION numpy_plus_one(INT)
RETURNS INT
LANGUAGE PYTHON
PACKAGES = ('numpy')
HANDLER = 'numpy_plus_one'
AS $$
import numpy as np

def numpy_plus_one(x: int) -> int:
    return int(np.add(x, 1))
$$
"""
    ctx.execute_query(create_numpy_udf_sql)
    print("Executing numpy UDF")
    ctx.check_result("SELECT numpy_plus_one(2) AS result", [["3"]])


def zip_whl_egg(ctx: SandboxContext):
    print("Creating zip/whl/egg UDF")
    create_pkg_udf_sql = f"""CREATE OR REPLACE FUNCTION read_pkg()
RETURNS STRING
LANGUAGE PYTHON
IMPORTS = ('@{ctx.udf_import_stage}/imports/lib.zip', '@{ctx.udf_import_stage}/imports/lib.whl', '@{ctx.udf_import_stage}/imports/lib.egg')
PACKAGES = ()
HANDLER = 'read_pkg'
AS $$
import zip_mod
import whl_mod
import egg_mod

def read_pkg() -> str:
    return f"{{zip_mod.VALUE}}-{{whl_mod.VALUE}}-{{egg_mod.VALUE}}"
$$
"""
    ctx.execute_query(create_pkg_udf_sql)
    print("Executing zip/whl/egg UDF")
    ctx.check_result("SELECT read_pkg() AS result", [["zip-whl-egg"]])


def read_stage(ctx: SandboxContext):
    print("Creating import reader UDF")
    create_reader_udf_sql = f"""CREATE OR REPLACE FUNCTION read_stage()
RETURNS STRING
LANGUAGE PYTHON
IMPORTS = (
  '@{ctx.udf_import_stage}/imports/helper.py',
  '@{ctx.udf_import_stage}/imports/data.txt',
  '@{ctx.udf_import_stage}/imports/subdir/extra.txt',
  '@{ctx.udf_import_stage}/imports/dup_a/dup.txt',
  '@{ctx.udf_import_stage}/imports/dup_b/dup.txt'
)
PACKAGES = ()
HANDLER = 'read_stage'
AS $$
import os
import sys
import helper

def read_stage() -> str:
    # Read staged files from sys._xoptions["databend_import_directory"].
    imports_dir = sys._xoptions["databend_import_directory"]
    with open(os.path.join(imports_dir, "data.txt"), "r") as f:
        content = f.read().strip()
    with open(os.path.join(imports_dir, "extra.txt"), "r") as f:
        extra = f.read().strip()
    # Same basename imports are flattened; last import wins for dup.txt.
    with open(os.path.join(imports_dir, "dup.txt"), "r") as f:
        dup = f.read().strip()
    return f"{{content}}:{{extra}}:{{dup}}:{{helper.add_one(1)}}"
$$
"""
    ctx.execute_query(create_reader_udf_sql)
    print("Executing read_stage UDF")
    ctx.check_result("SELECT read_stage() AS result", [["stage_data:extra_data:dup_b:2"]])


def read_archive(ctx: SandboxContext):
    print("Creating archive reader UDF")
    create_archive_udf_sql = f"""CREATE OR REPLACE FUNCTION read_archive()
RETURNS STRING
LANGUAGE PYTHON
IMPORTS = ('@{ctx.udf_import_stage}/imports/archive.tar.gz')
PACKAGES = ()
HANDLER = 'read_archive'
AS $$
import os
import sys
import tarfile

def read_archive() -> str:
    # Access tar.gz imports via sys._xoptions["databend_import_directory"].
    imports_dir = sys._xoptions["databend_import_directory"]
    archive_path = os.path.join(imports_dir, "archive.tar.gz")
    with tarfile.open(archive_path, "r:gz") as tf:
        member = tf.extractfile("archive.txt")
        content = member.read().decode("utf-8").strip()
    return content
$$
"""
    ctx.execute_query(create_archive_udf_sql)
    print("Executing read_archive UDF")
    ctx.check_result("SELECT read_archive() AS result", [["archive_data"]])


def presign_errors(ctx: SandboxContext):
    print("Checking presign error handling")
    create_missing_udf_sql = f"""CREATE OR REPLACE FUNCTION missing_import()
RETURNS INT
LANGUAGE PYTHON
IMPORTS = ('@{ctx.udf_import_stage}/imports/missing.py')
PACKAGES = ()
HANDLER = 'missing_import'
AS $$
def missing_import() -> int:
    return 1
$$
"""
    ctx.execute_query(create_missing_udf_sql)
    ctx.execute_query_expect_error(
        "SELECT missing_import() AS result", "Failed to stat UDF import"
    )

    bad_stage = "udf_import_stage_bad"
    create_bad_stage_sql = f"""CREATE STAGE IF NOT EXISTS {bad_stage}
URL = 's3://testbucket/udf-imports/'
CONNECTION = (
  access_key_id = '{ctx.s3_access_key}',
  secret_access_key = 'bad-secret',
  endpoint_url = '{ctx.s3_endpoint_url}'
);
"""
    ctx.execute_query(create_bad_stage_sql)

    create_bad_udf_sql = f"""CREATE OR REPLACE FUNCTION bad_creds()
RETURNS INT
LANGUAGE PYTHON
IMPORTS = ('@{bad_stage}/imports/helper.py')
PACKAGES = ()
HANDLER = 'bad_creds'
AS $$
def bad_creds() -> int:
    return 1
$$
"""
    ctx.execute_query(create_bad_udf_sql)
    ctx.execute_query_expect_error("SELECT bad_creds() AS result", "Failed to stat UDF import")


def find_status_url(log_path: Path, timeout: int = 30):
    pattern = re.compile(r"http://[^ ]+/health")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if log_path.exists():
            content = log_path.read_text(errors="ignore")
            matches = pattern.findall(content)
            if matches:
                return matches[-1]
        time.sleep(1)
    return ""


def check_health(url: str):
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        raise RuntimeError(f"[Test Error] health endpoint status code: {response.status_code}")
    if response.text != "1":
        raise RuntimeError(f"[Test Error] health endpoint returned unexpected body: {response.text}")


def check_stats(url: str):
    response = requests.get(url, timeout=10)
    data = response.json()
    if not all(key in data for key in ("request_count", "first_request_time", "last_request_time")):
        raise RuntimeError(f"[Test Error] stats endpoint returned invalid payload: {data}")
    if data.get("request_count", 0) < 1:
        raise RuntimeError(f"[Test Error] stats endpoint returned invalid payload: {data}")


def health_stats():
    print("Checking sandbox health and stats endpoints")
    status_url = find_status_url(ROOT / ".databend/cloud-control.out")
    if not status_url:
        raise RuntimeError("[Test Error] health endpoint not found in cloud-control.out")
    check_health(status_url)
    stats_url = status_url.replace("/health", "/stats")
    check_stats(stats_url)


def ensure_binaries(ctx: SandboxContext):
    meta_bin = ROOT / "target" / ctx.build_profile / "databend-meta"
    query_bin = ROOT / "target" / ctx.build_profile / "databend-query"
    if not meta_bin.exists():
        raise RuntimeError(f"missing binary: {meta_bin}")
    if not query_bin.exists():
        raise RuntimeError(f"missing binary: {query_bin}")


def main():
    ctx = SandboxContext()
    require_command("uv")
    require_command("docker")

    print("Cleaning up previous runs")
    run_command_optional(["killall", "-9", "databend-query"])
    run_command_optional(["killall", "-9", "databend-meta"])
    run_command_optional(["pkill", "-f", "tests/cloud_control_server/simple_server.py"])
    run_command_optional(["pkill", "-f", "uv run --project tests/cloud_control_server"])
    shutil.rmtree(ROOT / ".databend", ignore_errors=True)

    print("Pre-pulling base image")
    try:
        run_command(["docker", "pull", ctx.udf_docker_base_image])
    except subprocess.CalledProcessError:
        if docker_image_exists(ctx.udf_docker_base_image):
            print(
                "Warning: docker pull failed, but local image exists; continuing without pull."
            )
        else:
            raise

    print("Building runtime image")
    with tempfile.TemporaryDirectory() as tmp_dir:
        dockerfile = Path(tmp_dir) / "Dockerfile"
        dockerfile.write_text(
            "FROM {base}\n"
            "WORKDIR /app\n"
            "RUN python -m pip install --no-cache-dir uv\n"
            "RUN uv pip install --system databend-udf\n".format(base=ctx.udf_docker_base_image)
        )
        run_command(
            ["docker", "build", "-t", ctx.udf_docker_runtime_image, "-f", str(dockerfile), tmp_dir]
        )
    ctx.udf_docker_base_image = ctx.udf_docker_runtime_image

    for node in (1, 2, 3):
        config_file = ROOT / f"scripts/ci/deploy/config/databend-query-node-{node}.toml"
        apply_query_settings(config_file)

    ensure_binaries(ctx)

    print("Starting Databend Meta HA cluster (3 nodes)")
    (ROOT / ".databend").mkdir(parents=True, exist_ok=True)

    start_process(
        [
            str(ROOT / "target" / ctx.build_profile / "databend-meta"),
            "-c",
            "scripts/ci/deploy/config/databend-meta-node-1.toml",
        ],
        ROOT / ".databend/meta-1.out",
    )
    wait_port("127.0.0.1", 9191, 30)

    time.sleep(1)
    start_process(
        [
            str(ROOT / "target" / ctx.build_profile / "databend-meta"),
            "-c",
            "scripts/ci/deploy/config/databend-meta-node-2.toml",
        ],
        ROOT / ".databend/meta-2.out",
    )
    wait_port("127.0.0.1", 28202, 30)

    time.sleep(1)
    start_process(
        [
            str(ROOT / "target" / ctx.build_profile / "databend-meta"),
            "-c",
            "scripts/ci/deploy/config/databend-meta-node-3.toml",
        ],
        ROOT / ".databend/meta-3.out",
    )
    wait_port("127.0.0.1", 28302, 30)

    time.sleep(1)
    for node in (1, 2, 3):
        config_file = f"scripts/ci/deploy/config/databend-query-node-{node}.toml"
        port = 9090 + node
        print(f"Starting databend-query node-{node}")
        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "1"
        start_process(
            [
                str(ROOT / "target" / ctx.build_profile / "databend-query"),
                "-c",
                config_file,
            ],
            ROOT / f".databend/query-{node}.out",
            env=env,
        )
        wait_port("127.0.0.1", port, 30)
    wait_port("127.0.0.1", 8000, 30)

    print("Starting sandbox control mock server")
    env = os.environ.copy()
    env["UDF_DOCKER_BASE_IMAGE"] = ctx.udf_docker_base_image
    env["PYTHONUNBUFFERED"] = "1"
    start_process(
        [
            "uv",
            "run",
            "--project",
            "tests/cloud_control_server",
            "python",
            "tests/cloud_control_server/simple_server.py",
        ],
        ROOT / ".databend/cloud-control.out",
        env=env,
    )
    wait_port("127.0.0.1", 50051, 30)

    prepare_imports(ctx)
    metadata_imports(ctx)
    numpy_case(ctx)
    zip_whl_egg(ctx)
    read_stage(ctx)
    read_archive(ctx)
    presign_errors(ctx)
    health_stats()
    print("✅ Passed")


if __name__ == "__main__":
    main()
