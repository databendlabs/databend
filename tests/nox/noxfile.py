import json
import os
import shutil
import tarfile
import urllib.request
from pathlib import Path

import nox


PYTHON_DRIVER = ["0.33.7"]


@nox.session
@nox.parametrize("driver_version", PYTHON_DRIVER)
def python_client(session, driver_version):
    session.install("pytest")
    session.install(f"databend-driver=={driver_version}")
    session.run("pytest", "python_client")

    session.install("behave")
    with session.chdir("cache/bendsql/bindings/python"):
        env = {
            "DRIVER_VERSION": driver_version,
        }
        for impl in ['blocking', "asyncio", 'cursor']:
            session.run("behave", f"tests/{impl}", env=env)


JDBC_DRIVER = ["0.4.0", "main"]
GO_DRIVER_PINNED = ["v0.9.1"]
GO_DRIVER = ["main", "latest", *GO_DRIVER_PINNED]
GO_CLIENT_CACHE_DIR = Path(__file__).resolve().parent / "cache"
GO_CLIENT_ARCHIVE_ROOT = "https://github.com/databendlabs/databend-go/archive/refs"
GO_CLIENT_LATEST_RELEASE_URL = "https://github.com/databendlabs/databend-go/releases/latest"
GO_CLIENT_SKIP_UP = "DATABEND_GO_SKIP_UP"


@nox.session
@nox.parametrize("driver_version", JDBC_DRIVER)
def java_client(session, driver_version):
    main_ver = os.environ.get("JDBC_MAIN_VER")
    if main_ver is None or main_ver == "":
        raise Exception("evn JDBC_MAIN_VER should not be empty")

    session.install("requests")
    session.run("python", "java_client/prepare.py", driver_version)
    run_jdbc_test(session, driver_version, main_ver)


def run_jdbc_test(session, driver_version, main_version):
    main_target_dir = "cache/databend-jdbc/databend-jdbc/target"
    test_jar = f"{main_target_dir}/databend-jdbc-{main_version}-tests.jar"
    if driver_version == "main":
        main_jar = f"{main_target_dir}/databend-jdbc-{main_version}.jar"
        env = {}
    else:
        main_jar = f"cache/jdbc/databend-jdbc-{driver_version}.jar"
        env = {"DATABEND_JDBC_VERSION": driver_version}
    session.run(
        "java",
        "-Duser.timezone=Asia/Shanghai",
        "-cp",
        ":".join(["cache/lib/*", main_jar, test_jar]),
        "org.testng.TestNG",
        "java_client/testng.xml",
        external=True,
        env=env,
    )


def resolve_go_source_ref(source_ref):
    if source_ref != "latest":
        return source_ref

    request = urllib.request.Request(
        GO_CLIENT_LATEST_RELEASE_URL,
        headers={"User-Agent": "databend-nox-go-client"},
    )
    with urllib.request.urlopen(request) as response:
        latest_url = response.geturl().rstrip("/")

    latest_tag = latest_url.rsplit("/", 1)[-1]
    if not latest_tag.startswith("v"):
        raise RuntimeError(
            f"unexpected databend-go latest release redirect: {latest_url}"
        )

    return latest_tag


def get_go_archive_url(resolved_ref):
    archive_type = "tags" if resolved_ref.startswith("v") else "heads"
    return f"{GO_CLIENT_ARCHIVE_ROOT}/{archive_type}/{resolved_ref}.tar.gz"


def get_go_driver_env(resolved_ref):
    env = {}
    if resolved_ref != "main":
        env["DATABEND_GO_VERSION"] = resolved_ref

    if os.environ.get(GO_CLIENT_SKIP_UP):
        env[GO_CLIENT_SKIP_UP] = os.environ[GO_CLIENT_SKIP_UP]

    return env


def prepare_go_client_source(source_ref):
    resolved_ref = resolve_go_source_ref(source_ref)
    cache_key = source_ref.replace("/", "-")
    source_dir = GO_CLIENT_CACHE_DIR / f"databend-go-{cache_key}"
    marker_path = source_dir / ".databend-ref.json"
    makefile_path = source_dir / "tests" / "Makefile"

    if source_ref != "main" and marker_path.exists() and makefile_path.exists():
        marker = json.loads(marker_path.read_text())
        if marker.get("resolved_ref") == resolved_ref:
            return source_dir, resolved_ref

    shutil.rmtree(source_dir, ignore_errors=True)
    source_dir.mkdir(parents=True, exist_ok=True)

    request = urllib.request.Request(
        get_go_archive_url(resolved_ref),
        headers={"User-Agent": "databend-nox-go-client"},
    )
    with urllib.request.urlopen(request) as response:
        with tarfile.open(fileobj=response, mode="r|gz") as archive:
            archive.extractall(source_dir)

    extracted_dirs = [path for path in source_dir.iterdir() if path.is_dir()]
    if len(extracted_dirs) != 1:
        raise RuntimeError(
            f"expected one extracted databend-go root for {resolved_ref}, got {len(extracted_dirs)}"
        )

    extracted_root = extracted_dirs[0]
    for child in extracted_root.iterdir():
        shutil.move(str(child), source_dir / child.name)
    extracted_root.rmdir()

    if not makefile_path.exists():
        raise RuntimeError(
            f"missing databend-go tests/Makefile after extracting {resolved_ref}"
        )

    marker_path.write_text(json.dumps({"resolved_ref": resolved_ref}) + "\n")
    return source_dir, resolved_ref




@nox.session
@nox.parametrize("source_ref", GO_DRIVER)
def go_client(session, source_ref):
    source_dir, resolved_ref = prepare_go_client_source(source_ref)
    env = get_go_driver_env(resolved_ref)
    test_dir = source_dir / "tests"
    skip_up = os.environ.get(GO_CLIENT_SKIP_UP)
    make_args = ["make"]
    if skip_up:
        make_args.extend(["-o", "up"])
    make_args.append("integration")

    session.log(f"running databend-go integration tests from {resolved_ref}")
    with session.chdir(str(test_dir)):
        session.run(*make_args, external=True, env=env)

# test API with requests directly.
# some of the tests will fail with cluster behind nginx.
# so run it in .github/actions/test_stateful_cluster_linux/action.yml.
@nox.session
def test_suites(session):
    session.install(
        "pytest",
        "requests",
        "pytest-asyncio",
        "pyarrow",
        "databend-driver",
        "pylance",
    )
    # Usage: nox -s test_suites -- suites/http_handler/test_session.py::test_session
    session.run("pytest", *session.posargs)


@nox.session
def udf_sandbox(session):
    session.install("requests")
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    with session.chdir(repo_root):
        session.run("python", "tests/udf/sandbox_udf.py", *session.posargs)
