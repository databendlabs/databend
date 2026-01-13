import nox
import os


PYTHON_DRIVER = ["0.33.1"]


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
        # for impl in ['blocking', "asyncio", 'cursor']:
        #     session.run("behave", f"tests/{impl}", env=env)


JDBC_DRIVER = ["0.4.0", "main"]


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


@nox.session
def test_suites(session):
    session.install("pytest", "requests", "pytest-asyncio", "pyarrow", "databend-driver")
    # Usage: nox -s test_suites -- suites/1_stateful/09_http_handler/test_09_0007_session.py::test_session
    session.run("pytest", *session.posargs)


@nox.session
@nox.parametrize("driver_version", ["v100.0.0", "v0.8.3"])
def go_client(session, driver_version):
    env = {"DATABEND_GO_VERSION": driver_version}
    test_dir = f"cache/databend-go/tests"
    with session.cd(test_dir):
        if os.path.exists("go.mod"):
            os.remove("go.mod")
        if driver_version == "v100.0.0":
            session.run("make", "-o", "up", "integration", external=True, env=env)
        else:
            session.run("make", "-o", "up", "compat", external=True, env=env)
