import nox


DRIVER_VERSIONS = ["0.28.1", "0.28.2"]


@nox.session
@nox.parametrize("driver_version", DRIVER_VERSIONS)
def python_client(session, driver_version):

    session.install("pytest")
    session.install(f"databend-driver=={driver_version}")
    session.run("pytest", "-vv", "python_client")
