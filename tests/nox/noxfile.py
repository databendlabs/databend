import json
import os
import shutil
import tarfile
import tempfile
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import nox


PYTHON_DRIVER = ["0.33.7"]
CACHE_DIR = Path(__file__).resolve().parent / "cache"
GITHUB_ARCHIVE_ROOT = "https://github.com/databendlabs"
HTTP_USER_AGENT = "databend-nox-client"

JDBC_DRIVER_PINNED = ["0.4.1"]
JDBC_DRIVER = ["main", "latest", *JDBC_DRIVER_PINNED]
JDBC_SOURCE_REPO = "databend-jdbc"
JDBC_RELEASE_LATEST_URL = (
    "https://github.com/databendlabs/databend-jdbc/releases/latest"
)
JDBC_RELEASE_DOWNLOAD_ROOT = (
    "https://github.com/databendlabs/databend-jdbc/releases/download"
)
JDBC_SOURCE_BUILD_ENV = {"TEST_HANDLERS": "http"}
JDBC_EXCLUDED_GROUPS = "FLAKY,cluster,MULTI_HOST"
JDBC_MAIN_TEST_ARGS = [
    "-pl",
    "databend-jdbc",
    "test",
    "-Dgroups=IT",
    "-DexcludedGroups=FLAKY",
]
JDBC_TEST_LIBS = [
    "https://repo.maven.apache.org/maven2/org/testng/testng/7.11.0/testng-7.11.0.jar",
    "https://repo1.maven.org/maven2/com/vdurmont/semver4j/3.1.0/semver4j-3.1.0.jar",
    "https://repo1.maven.org/maven2/org/jcommander/jcommander/1.83/jcommander-1.83.jar",
    "https://repo1.maven.org/maven2/org/locationtech/jts/jts-core/1.19.0/jts-core-1.19.0.jar",
    "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar",
    "https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.13/slf4j-simple-2.0.13.jar",
    "https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/1.11.3/junit-platform-console-standalone-1.11.3.jar",
]

GO_DRIVER_PINNED = ["v0.9.1"]
GO_DRIVER = ["main", "latest", *GO_DRIVER_PINNED]
GO_CLIENT_ARCHIVE_ROOT = f"{GITHUB_ARCHIVE_ROOT}/databend-go/archive/refs"
GO_CLIENT_LATEST_RELEASE_URL = "https://github.com/databendlabs/databend-go/releases/latest"
GO_CLIENT_SKIP_UP = "DATABEND_GO_SKIP_UP"


def get_request(url):
    return urllib.request.Request(url, headers={"User-Agent": HTTP_USER_AGENT})


def resolve_latest_release_tag(latest_release_url, expected_prefix="v"):
    with urllib.request.urlopen(get_request(latest_release_url)) as response:
        latest_url = response.geturl().rstrip("/")

    latest_tag = latest_url.rsplit("/", 1)[-1]
    if expected_prefix and not latest_tag.startswith(expected_prefix):
        raise RuntimeError(f"unexpected latest release redirect: {latest_url}")
    return latest_tag


def get_archive_url(archive_root, resolved_ref):
    archive_type = "tags" if resolved_ref.startswith("v") else "heads"
    return f"{archive_root}/{archive_type}/{resolved_ref}.tar.gz"


def prepare_source_archive(
    source_name, source_ref, resolved_ref, archive_root, required_path
):
    cache_key = source_ref.replace("/", "-")
    source_dir = CACHE_DIR / f"{source_name}-{cache_key}"
    marker_path = source_dir / ".databend-ref.json"
    required_file = source_dir / required_path

    if source_ref != "main" and marker_path.exists() and required_file.exists():
        marker = json.loads(marker_path.read_text())
        if marker.get("resolved_ref") == resolved_ref:
            return source_dir

    shutil.rmtree(source_dir, ignore_errors=True)
    source_dir.mkdir(parents=True, exist_ok=True)

    with urllib.request.urlopen(get_request(get_archive_url(archive_root, resolved_ref))) as response:
        with tarfile.open(fileobj=response, mode="r|gz") as archive:
            archive.extractall(source_dir)

    extracted_dirs = [path for path in source_dir.iterdir() if path.is_dir()]
    if len(extracted_dirs) != 1:
        raise RuntimeError(
            f"expected one extracted {source_name} root for {resolved_ref}, got {len(extracted_dirs)}"
        )

    extracted_root = extracted_dirs[0]
    for child in extracted_root.iterdir():
        shutil.move(str(child), source_dir / child.name)
    extracted_root.rmdir()

    if not required_file.exists():
        raise RuntimeError(
            f"missing {required_path} after extracting {source_name} {resolved_ref}"
        )

    marker_path.write_text(json.dumps({"resolved_ref": resolved_ref}) + "\n")
    return source_dir


def download_file(url, target):
    if target.exists():
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(get_request(url)) as response:
        with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as temp_file:
            shutil.copyfileobj(response, temp_file)
            temp_path = Path(temp_file.name)

    temp_path.replace(target)
    return target


def merge_env(*env_sets):
    env = {}
    for item in env_sets:
        if item:
            env.update(item)
    return env


def http_query(sql, port=8000):
    payload = json.dumps({"sql": sql}).encode()
    request = urllib.request.Request(
        f"http://localhost:{port}/v1/query/",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic cm9vdDo=",
        },
        method="POST",
    )

    with urllib.request.urlopen(request) as response:
        result = json.loads(response.read())

    if result["error"]:
        raise RuntimeError(f"query failed for {sql}: {result['error']}")
    return result


def create_jdbc_test_user():
    try:
        http_query("DROP USER IF EXISTS databend")
    except RuntimeError as e:
        print(e)
        return

    http_query(
        "CREATE USER databend IDENTIFIED BY 'databend' with default_role='account_admin'"
    )
    http_query("GRANT ROLE account_admin TO USER databend")
    http_query("SYSTEM FLUSH PRIVILEGES")


def prepare_jdbc_test_libs():
    for url in JDBC_TEST_LIBS:
        filename = url.rsplit("/", 1)[-1]
        download_file(url, CACHE_DIR / "lib" / filename)


def resolve_jdbc_driver_ref(source_ref):
    if source_ref != "latest":
        return source_ref

    return resolve_latest_release_tag(JDBC_RELEASE_LATEST_URL).removeprefix("v")


def get_jdbc_release_asset(version, suffix=""):
    filename = f"databend-jdbc-{version}{suffix}.jar"
    target = CACHE_DIR / "jdbc" / filename
    url = f"{JDBC_RELEASE_DOWNLOAD_ROOT}/v{version}/{filename}"
    return download_file(url, target)


def get_jdbc_source_version(source_dir):
    pom_path = source_dir / "databend-jdbc" / "pom.xml"
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    root = ET.parse(pom_path).getroot()
    version = root.findtext("m:version", namespaces=namespace)
    if version:
        return version.strip()

    parent_version = root.findtext("m:parent/m:version", namespaces=namespace)
    if parent_version:
        return parent_version.strip()

    raise RuntimeError(f"failed to determine databend-jdbc version from {pom_path}")


def prepare_jdbc_main_source(session):
    source_dir = prepare_source_archive(
        source_name=JDBC_SOURCE_REPO,
        source_ref="main",
        resolved_ref="main",
        archive_root=f"{GITHUB_ARCHIVE_ROOT}/{JDBC_SOURCE_REPO}/archive/refs",
        required_path="databend-jdbc/pom.xml",
    )
    source_version = get_jdbc_source_version(source_dir)

    return {
        "resolved_ref": "main",
        "driver_version": source_version,
        "source_dir": source_dir,
        "env": {},
    }


def prepare_jdbc_release_artifacts(source_ref):
    resolved_ref = resolve_jdbc_driver_ref(source_ref)
    return {
        "resolved_ref": resolved_ref,
        "driver_version": resolved_ref,
        "main_jar": get_jdbc_release_asset(resolved_ref),
        "test_jar": get_jdbc_release_asset(resolved_ref, "-tests"),
        "suite_path": None,
        "suite_test_names": [],
        "env": {"DATABEND_JDBC_VERSION": resolved_ref},
    }


def get_test_class_name(class_entry):
    if not class_entry.endswith(".class") or "$" in class_entry:
        return None

    class_name = class_entry.removesuffix(".class").replace("/", ".")
    simple_name = class_name.rsplit(".", 1)[-1]
    if simple_name in {"Compatibility", "Utils"}:
        return None
    if not (simple_name.startswith("Test") or simple_name.endswith("Test")):
        return None
    return class_name


def discover_jdbc_test_classes(test_jar):
    with zipfile.ZipFile(test_jar) as jar_file:
        classes = {
            class_name
            for class_name in (
                get_test_class_name(entry.filename) for entry in jar_file.infolist()
            )
            if class_name is not None
        }

    if not classes:
        raise RuntimeError(f"no test classes discovered in {test_jar}")
    return sorted(classes)


def build_jdbc_testng_suite(test_jar):
    suite_path = CACHE_DIR / "generated" / f"{test_jar.stem}-testng.xml"
    suite_path.parent.mkdir(parents=True, exist_ok=True)

    suite = ET.Element("suite", {"name": "DatabendJdbcTests", "verbose": "10", "parallel": "none"})
    test = ET.SubElement(suite, "test", {"name": "AllTests"})
    groups = ET.SubElement(test, "groups")
    run = ET.SubElement(groups, "run")
    ET.SubElement(run, "include", {"name": "IT"})
    for excluded_group in JDBC_EXCLUDED_GROUPS.split(","):
        ET.SubElement(run, "exclude", {"name": excluded_group})

    classes = ET.SubElement(test, "classes")
    for class_name in discover_jdbc_test_classes(test_jar):
        ET.SubElement(classes, "class", {"name": class_name})

    suite_xml = ET.tostring(suite, encoding="unicode")
    suite_path.write_text(
        '<!DOCTYPE suite SYSTEM "https://testng.org/testng-1.1.dtd" >\n'
        + suite_xml
        + "\n"
    )
    return suite_path


def prepare_jdbc_test_output_dir(resolved_ref):
    output_dir = CACHE_DIR / "test-output" / f"jdbc-{resolved_ref}"
    shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def run_jdbc_release_test(session, jdbc_target):
    testng_suite = build_jdbc_testng_suite(jdbc_target["test_jar"])
    output_dir = prepare_jdbc_test_output_dir(jdbc_target["resolved_ref"])
    session.run(
        "java",
        "-Duser.timezone=Asia/Shanghai",
        "-cp",
        os.pathsep.join(
            [
                "cache/lib/*",
                str(jdbc_target["main_jar"]),
                str(jdbc_target["test_jar"]),
            ]
        ),
        "org.testng.TestNG",
        "-d",
        str(output_dir),
        str(testng_suite),
        external=True,
        env=jdbc_target["env"],
    )


def run_jdbc_main_test(session, jdbc_target):
    with session.chdir(str(jdbc_target["source_dir"])):
        session.run(
            "mvn",
            *JDBC_MAIN_TEST_ARGS,
            external=True,
            env=merge_env(jdbc_target["env"]),
        )


def resolve_go_source_ref(source_ref):
    if source_ref != "latest":
        return source_ref

    return resolve_latest_release_tag(GO_CLIENT_LATEST_RELEASE_URL)


def get_go_driver_env(resolved_ref):
    env = {}
    if resolved_ref != "main":
        env["DATABEND_GO_VERSION"] = resolved_ref

    if os.environ.get(GO_CLIENT_SKIP_UP):
        env[GO_CLIENT_SKIP_UP] = os.environ[GO_CLIENT_SKIP_UP]

    return env


def prepare_go_client_source(source_ref):
    resolved_ref = resolve_go_source_ref(source_ref)
    source_dir = prepare_source_archive(
        source_name="databend-go",
        source_ref=source_ref,
        resolved_ref=resolved_ref,
        archive_root=GO_CLIENT_ARCHIVE_ROOT,
        required_path="tests/Makefile",
    )
    return source_dir, resolved_ref


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
        for impl in ["blocking", "asyncio", "cursor"]:
            session.run("behave", f"tests/{impl}", env=env)


@nox.session
@nox.parametrize("driver_version", JDBC_DRIVER)
def java_client(session, driver_version):
    create_jdbc_test_user()

    if driver_version == "main":
        jdbc_target = prepare_jdbc_main_source(session)
        session.log("running databend-jdbc integration tests from main via Maven")
        run_jdbc_main_test(session, jdbc_target)
    else:
        prepare_jdbc_test_libs()
        jdbc_target = prepare_jdbc_release_artifacts(driver_version)
        session.log(
            f"running databend-jdbc integration tests from {jdbc_target['resolved_ref']}"
        )
        run_jdbc_release_test(session, jdbc_target)


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
        session.run(*make_args, external=True, env=merge_env(env))


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
        "PyJWT",
        "cryptography",
    )
    # Usage: nox -s test_suites -- suites/http_handler/test_session.py::test_session
    session.run("pytest", *session.posargs)


@nox.session
def udf_sandbox(session):
    session.install("requests")
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    with session.chdir(repo_root):
        session.run("python", "tests/udf/sandbox_udf.py", *session.posargs)
