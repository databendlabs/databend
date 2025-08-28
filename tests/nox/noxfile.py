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
    session.run("python", "java_client/download.py", driver_version)
    session.run(
        "java",
        "-cp",
        f"cache/jdbc/databend-jdbc-{driver_version}.jar",
        "java_client/test.java",
        external=True,
    )
