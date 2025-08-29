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
    session.run(
        "java",
        "-cp",
        f"cache/jdbc/databend-jdbc-{driver_version}.jar",
        "java_client/test.java",
        external=True,
    )

    # session.run(
    #     "java",
    #     "-cp",
    #     ":".join(
    #         [
    #             "cache/lib/jcommander-1.83.jar",
    #             "cache/lib/testng-7.11.0.jar",
    #             "cache/lib/slf4j-api-2.0.16.jar",
    #             "cache/lib/slf4j-simple-2.0.13.jar",
    #             "cache/lib/semver4j-3.1.0.jar",
    #             "cache/lib/jts-core-1.19.0.jar",
    #             "cache/lib/junit-platform-console-standalone-1.11.3.jar"
    #             f"cache/jdbc/databend-jdbc-{driver_version}.jar",
    #             f"cache/jdbc/databend-jdbc-{driver_version}-tests.jar",
    #         ]
    #     ),
    #     "org.testng.TestNG",
    #     "java_client/testng.xml",
    #     external=True,
    # )
