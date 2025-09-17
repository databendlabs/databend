import nox
import os


PYTHON_DRIVER = ["0.28.1", "0.28.2"]


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
        session.run("behave", "tests/asyncio", env=env)
        session.run("behave", "tests/blocking", env=env)
        session.run("behave", "tests/cursor", env=env)


JDBC_DRIVER = ["0.4.0", "main"]


@nox.session
@nox.parametrize("driver_version", JDBC_DRIVER)
def java_client(session, driver_version):
    main_ver = os.environ.get("JDBC_MAIN_VER")
    if main_ver is None or main_ver == "":
        raise Exception("evn JDBC_MAIN_VER should not be empty")

    session.install("requests")
    session.run("python", "java_client/perpare.py", driver_version)
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
        "-cp",
        ":".join(["cache/lib/*", main_jar, test_jar]),
        "org.testng.TestNG",
        "java_client/testng.xml",
        external=True,
        env=env,
    )


@nox.session
def test_suites(session):
    session.install("pytest", "requests", "pytest-asyncio")
    # Usage: nox -s test_suites -- suites/1_stateful/09_http_handler/test_09_0007_session.py::test_session
    session.run("pytest", *session.posargs)


@nox.session
@nox.parametrize("driver_version", ["100.0.0", "0.8.3"])
def go_client(session, driver_version):
    env = {"DATABEND_GO_VERSION": driver_version}
    test_dir = f"cache/databend-go/tests"
    with session.cd(test_dir):
        if driver_version == "100.0.0":
            if os.path.exists("go.mod"):
                os.remove("go.mod")
        else:
            session.run("go", "mod", "init", f"it", external=True)
            session.run(
                "go",
                "get",
                f"github.com/datafuselabs/databend-go@v{driver_version}",
                external=True,
            )
            session.run("go", "get", "-t", "./", external=True)

        session.run(
            "go",
            "test",
            "-v",
            "-timeout",
            "5m",
            "./",
            external=True,
            env=env,
        )
