import nox


PYTHON_DRIVER = ["0.28.1", "0.28.2"]


@nox.session
@nox.parametrize("driver_version", PYTHON_DRIVER)
def python_client(session, driver_version):
    session.install("pytest")
    session.install(f"databend-driver=={driver_version}")
    session.run("pytest", "python_client")


JDBC_DRIVER = ["0.4.0"]


@nox.session
@nox.parametrize("driver_version", JDBC_DRIVER)
def java_client(session, driver_version):
    session.install("requests")
    session.run("python", "java_client/perpare.py", driver_version)
    run_jdbc_test(session, driver_version)


def run_jdbc_test(session, driver_version):
    session.run(
        "java",
        "-cp",
        ":".join(
            [
                "cache/lib/*",
                f"cache/jdbc/databend-jdbc-{driver_version}.jar",
                f"cache/jdbc/databend-jdbc-{driver_version}-tests.jar",
            ]
        ),
        "org.testng.TestNG",
        "java_client/testng.xml",
        external=True,
    )
