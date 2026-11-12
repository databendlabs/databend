"""HTTP query ownership, session isolation and heartbeat forwarding.

Run with: nox -f tests/nox/noxfile.py -s test_suites --
    suites/http_handler/test_query_owner.py
Remote cases use the standard cluster's second query node on port 8002.
"""

import time
import uuid
from contextlib import contextmanager

import pytest
import requests

BASE_URL = "http://localhost:8000"
PASSWORD = "query_owner_password"
SESSION_ID = "X-DATABEND-SESSION-ID"
STICKY = "X-DATABEND-STICKY-NODE"


def client(auth):
    session = requests.Session()
    # The local test deployment must not go through an HTTP proxy.
    session.trust_env = False
    session.auth = auth
    return session


def request(session, method, path, base_url=BASE_URL, **kwargs):
    return session.request(method, base_url + path, timeout=30, **kwargs)


def command(session, sql):
    response = request(session, "POST", "/v1/query", json={"sql": sql})
    response.raise_for_status()
    body = response.json()
    rows = list(body.get("data", []))
    for _ in range(100):
        assert body.get("error") is None, body
        uri = body.get("next_uri")
        if not uri:
            return rows
        response = request(session, "GET", uri)
        response.raise_for_status()
        body = response.json()
        rows.extend(body.get("data", []))
    pytest.fail("SQL command did not finish within 100 pages")


@pytest.fixture
def users():
    suffix = uuid.uuid4().hex
    names = [f"http_owner_{suffix}", f"http_other_{suffix}"]
    with client(("root", "")) as root:
        try:
            for name in names:
                command(root, f"CREATE USER '{name}' IDENTIFIED BY '{PASSWORD}'")
                command(root, f"GRANT SELECT ON *.* TO '{name}'")
            yield [(name, PASSWORD) for name in names]
        finally:
            for name in names:
                command(root, f"DROP USER IF EXISTS '{name}'")


@contextmanager
def owner_client(auth, mode):
    with client(auth) as session:
        if mode in ("cookie", "token"):
            session.cookies.set("cookie_enabled", "true")
        elif mode == "header":
            session.headers["X-DATABEND-CLIENT-CAPS"] = "session_header"
        elif mode == "worksheet":
            session.headers["User-Agent"] = "DatabendCloud/worksheet=owner-test"
            session.headers[SESSION_ID] = str(uuid.uuid4())
        if mode != "none":
            response = request(session, "POST", "/v1/session/login", json={})
            response.raise_for_status()
            body = response.json()
            assert body.get("error") is None, body
            if mode == "header":
                session.headers["X-DATABEND-SESSION"] = response.headers[
                    "X-DATABEND-SESSION"
                ]
            if mode == "token":
                session.auth = None
                session.headers["Authorization"] = (
                    f"Bearer {body['tokens']['session_token']}"
                )
        yield session


@contextmanager
def running_query(session, base_url=BASE_URL, sql=None, result_timeout=60):
    query_id = f"http_owner_{uuid.uuid4().hex}"
    response = request(
        session,
        "POST",
        "/v1/query",
        base_url,
        headers={"x-databend-query-id": query_id},
        json={
            "sql": sql or "SELECT sleep(0.5), number FROM numbers(15000000000)",
            "pagination": {
                "wait_time_secs": 1,
                "max_rows_per_page": 1,
                "max_rows_in_buffer": 2,
            },
            "session": {
                "settings": {
                    "http_handler_result_timeout_secs": str(result_timeout),
                    "max_threads": "2",
                }
            },
        },
    )
    response.raise_for_status()
    body = response.json()
    assert body.get("error") is None, body
    assert body["id"] == query_id, body
    if sql is None:
        assert body["state"] == "Running", body
    try:
        yield body
    finally:
        response = request(session, "GET", body["final_uri"], base_url)
        response.raise_for_status()


def state(session, query, base_url=BASE_URL):
    response = request(session, "GET", f"/v1/query/{query['id']}", base_url)
    response.raise_for_status()
    return response.json()


def assert_denied(response, owner_name, session_id):
    assert response.status_code == 401, response.text
    assert owner_name not in response.text, response.text
    if session_id:
        assert session_id not in response.text, response.text
    assert "expected" not in response.text, response.text


@pytest.mark.parametrize("mode", ["none", "cookie", "header", "worksheet", "token"])
@pytest.mark.parametrize("kill_method", ["GET", "POST"])
def test_query_owner(users, mode, kill_method):
    owner_auth, other_auth = users
    with (
        owner_client(owner_auth, mode) as owner,
        client(other_auth) as other,
        client(owner_auth) as different_session,
        client(owner_auth) as no_session,
        client(("root", "")) as root,
        running_query(owner) as query,
    ):
        session_id = query.get("session_id")
        query_path = f"/v1/query/{query['id']}"
        for caller in (other, root):
            # A client-supplied query ID must not bypass ownership on retry.
            response = request(
                caller,
                "POST",
                "/v1/query",
                headers={"x-databend-query-id": query["id"]},
                json={"sql": "SELECT 1"},
            )
            assert_denied(response, owner_auth[0], session_id)
            # SUPER is deliberately not a bypass for the HTTP query API.
            for method, suffix in [
                ("GET", ""),
                ("GET", "/page/0"),
                ("GET", "/kill"),
                ("POST", "/kill"),
                ("GET", "/final"),
                ("POST", "/final"),
            ]:
                response = request(caller, method, query_path + suffix)
                assert_denied(response, owner_auth[0], session_id)
                assert state(owner, query)["state"] == "Running"

        if mode != "none":
            assert session_id, query
            assert state(different_session, query)["state"] == "Running"
            for suffix in ("/page/0", "/final"):
                response = request(different_session, "GET", query_path + suffix)
                assert_denied(response, owner_auth[0], session_id)
                assert state(owner, query)["state"] == "Running"
            # A client-supplied cookie is not proof of query ownership.
            other.cookies.set("cookie_enabled", "true")
            other.cookies.set("session_id", session_id)
            for suffix in ("/kill", "/final", "", "/page/0"):
                response = request(
                    other, "POST" if suffix == "/kill" else "GET", query_path + suffix
                )
                assert_denied(response, owner_auth[0], session_id)
                assert state(owner, query)["state"] == "Running"

        different_session.cookies.set("cookie_enabled", "true")
        different_session.cookies.set("session_id", str(uuid.uuid4()))
        assert state(different_session, query)["state"] == "Running"
        response = request(different_session, "POST", query["final_uri"])
        assert_denied(response, owner_auth[0], session_id)
        assert state(owner, query)["state"] == "Running"

        # Both missing and different sessions are allowed for the same owner.
        assert state(no_session, query)["state"] == "Running"
        for caller in (owner, no_session, different_session):
            response = request(
                caller,
                "POST",
                "/v1/query",
                headers={"x-databend-query-id": query["id"]},
                json={"sql": "SELECT 1"},
            )
            assert response.status_code == 200, response.text
            retry = response.json()
            assert retry.get("error") is None, retry
            assert retry["id"] == query["id"], retry
            assert retry["data"] == query["data"], retry
        caller = no_session if kill_method == "GET" else different_session
        response = request(caller, kill_method, query["kill_uri"])
        assert response.status_code == 200, response.text
        response = request(owner, "GET", query["final_uri"])
        assert response.status_code == 200, response.text
        assert "canceled" in response.json()["error"]["message"], response.text
        # Closed queries must remain protected too.
        response = request(other, "GET", query_path)
        assert_denied(response, owner_auth[0], session_id)

    # Finalizing a still-running query also works for its owner.
    with owner_client(owner_auth, mode) as owner, running_query(owner) as query:
        response = request(owner, "POST", query["final_uri"])
        assert response.status_code == 200, response.text
        assert response.json()["next_uri"] is None, response.text


def heartbeat(session, queries):
    node_to_queries = {}
    for query in queries:
        node_to_queries.setdefault(query["node_id"], []).append(query["id"])
    response = request(
        session,
        "POST",
        "/v1/session/heartbeat",
        json={"node_to_queries": node_to_queries},
    )
    response.raise_for_status()
    return response.json()["queries_to_remove"]


def query_node(root, remote):
    if remote:
        if int(command(root, "SELECT count(*) FROM system.clusters")[0][0]) < 2:
            pytest.skip("requires the multi-node test deployment")
        return "http://localhost:8002"
    return BASE_URL


@pytest.mark.parametrize("remote", [False, True], ids=["local", "forwarded"])
@pytest.mark.parametrize("mode", ["none", "cookie", "header", "worksheet", "token"])
def test_heartbeat_query_owner(users, remote, mode):
    owner_auth, other_auth = users
    with (
        client(("root", "")) as root,
        owner_client(owner_auth, mode) as owner,
        client(other_auth) as other,
        client(owner_auth) as different_session,
    ):
        base_url = query_node(root, remote)
        with running_query(owner, base_url) as query:
            assert heartbeat(other, [query]) == [query["id"]]
            assert heartbeat(root, [query]) == [query["id"]]
            if mode != "none":
                assert heartbeat(different_session, [query]) == []
                different_session.cookies.set("cookie_enabled", "true")
                different_session.cookies.set("session_id", str(uuid.uuid4()))
                assert heartbeat(different_session, [query]) == []
                other.cookies.set("cookie_enabled", "true")
                other.cookies.set("session_id", query["session_id"])
                assert heartbeat(other, [query]) == [query["id"]]
            assert heartbeat(owner, [query]) == []
            assert state(owner, query, base_url)["state"] == "Running"


@pytest.mark.parametrize("remote", [False, True], ids=["local", "forwarded"])
def test_unauthorized_heartbeat_does_not_extend_timeout(users, remote):
    owner_auth, other_auth = users
    with (
        client(("root", "")) as root,
        owner_client(owner_auth, "cookie") as owner,
        owner_client(owner_auth, "cookie") as different_session,
        client(other_auth) as other,
    ):
        base_url = query_node(root, remote)
        # Leave unread results so an authorized heartbeat would extend their TTL.
        with running_query(
            owner, base_url, sql="SELECT * FROM numbers(100)", result_timeout=3
        ) as query:
            # The same owner can extend TTL from another session, including
            # across forwarding with cookies. State polling does not refresh TTL.
            assert owner.cookies.get("session_id") != different_session.cookies.get(
                "session_id"
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                assert heartbeat(different_session, [query]) == []
                time.sleep(0.5)
            assert state(owner, query, base_url)["state"] != "Failed"

            other.cookies.set("cookie_enabled", "true")
            other.cookies.set("session_id", query["session_id"])
            # Past the 3s result timeout, but before the closed query is removed.
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                assert heartbeat(other, [query]) == [query["id"]]
                time.sleep(0.5)
            response = request(
                owner,
                "GET",
                query["next_uri"],
                base_url,
                headers={STICKY: query["node_id"]},
            )
            assert response.status_code == 400, response.text
            assert "timed out" in response.text, response.text
