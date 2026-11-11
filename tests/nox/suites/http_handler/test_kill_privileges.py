"""SQL KILL permissions, including Flight forwarding in the standard test cluster.

Run with: nox -s test_suites -- suites/http_handler/test_kill_privileges.py
The cluster deployment exposes a second node on port 8002 (as in test_forward.py).
These tests use prefixed session IDs and query IDs, not the HTTP query-kill API.
"""

import time
import uuid

import pytest
import requests

ROOT = ("root", "")
BASE_URL = "http://localhost:8000"


def query(sql, auth=ROOT, base_url=BASE_URL, query_id=None, wait=5):
    headers = {}
    if query_id is not None:
        headers["x-databend-query-id"] = query_id
    response = requests.post(
        f"{base_url}/v1/query",
        auth=auth,
        headers=headers,
        json={
            "sql": sql,
            "pagination": {
                "wait_time_secs": wait,
                "max_rows_per_page": 2,
                "max_rows_in_buffer": 2,
            },
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def command(sql, auth=ROOT, base_url=BASE_URL):
    result = query(sql, auth, base_url)
    rows = list(result.get("data", []))
    while result.get("next_uri"):
        response = requests.get(base_url + result["next_uri"], auth=auth, timeout=30)
        response.raise_for_status()
        result = response.json()
        rows.extend(result.get("data", []))
    if result.get("final_uri"):
        requests.get(
            base_url + result["final_uri"], auth=auth, timeout=30
        ).raise_for_status()
    return result, rows


def succeed(sql, auth=ROOT, base_url=BASE_URL):
    result, rows = command(sql, auth, base_url)
    assert result.get("error") is None, result
    assert result["state"] == "Succeeded", result
    return rows


@pytest.fixture
def users():
    suffix = uuid.uuid4().hex
    u1, u2 = f"kill_u1_{suffix}", f"kill_u2_{suffix}"
    for user in (u1, u2):
        succeed(f"CREATE USER '{user}' IDENTIFIED BY 'kill_test_password'")
        succeed(f"GRANT SELECT ON *.* TO '{user}'")
    try:
        yield (u1, "kill_test_password"), (u2, "kill_test_password")
    finally:
        for user in (u1, u2):
            succeed(f"DROP USER IF EXISTS '{user}'")


def query_state(query_id, auth):
    response = requests.get(f"{BASE_URL}/v1/query/{query_id}", auth=auth, timeout=30)
    response.raise_for_status()
    return response.json()


@pytest.mark.parametrize(
    "kind,id_type",
    [("QUERY", "session"), ("CONNECTION", "session"), ("QUERY", "query")],
)
@pytest.mark.parametrize("killer", ["owner", "root"])
@pytest.mark.parametrize("remote", [False, True], ids=["local", "remote"])
def test_sql_kill_privileges(users, kind, id_type, killer, remote):
    kill_url = BASE_URL
    if remote:
        nodes = succeed("SELECT count(*) FROM system.clusters")
        if int(nodes[0][0]) < 2:
            pytest.skip("requires the multi-node test deployment")
        kill_url = "http://localhost:8002"

    owner, other = users
    query_id = uuid.uuid4().hex
    started = query(
        "SELECT sleep(0.5), number FROM numbers(15000000000)",
        owner,
        query_id=query_id,
        wait=0,
    )
    session_id = None
    try:
        # State-only polling responses do not include pagination URIs.
        next_uri = started.get("next_uri")
        # wait=0 can return before asynchronous planning and execution have started.
        deadline = time.monotonic() + 15
        while started["state"] == "Starting" and time.monotonic() < deadline:
            time.sleep(0.1)
            started = query_state(query_id, owner)
        assert started["state"] == "Running", started

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            rows = succeed(
                f"SELECT id FROM system.processes WHERE current_query_id = '{query_id}'"
            )
            if rows:
                session_id = rows[0][0]
                break
            time.sleep(0.1)
        assert session_id is not None, "running query has no session"
        assert session_id.startswith("session-"), session_id
        assert str(
            uuid.UUID(session_id.removeprefix("session-"))
        ) == session_id.removeprefix("session-")
        target_id = query_id if id_type == "query" else session_id

        if id_type == "query":
            rejected, _ = command(f"KILL CONNECTION '{query_id}'", owner, kill_url)
            assert rejected["error"]["code"] == 1006, rejected  # BadArguments
            assert "not a query id" in rejected["error"]["message"], rejected

        denied, _ = command(f"KILL {kind} '{target_id}'", other, kill_url)
        assert denied["error"]["code"] == 1063, denied  # PermissionDenied
        assert "not owner of target" in denied["error"]["message"], denied
        assert query_state(query_id, owner)["state"] == "Running"
        assert succeed(
            f"SELECT id FROM system.processes WHERE current_query_id = '{query_id}'"
        ) == [[session_id]]

        # A fresh HTTP session of the same user is sufficient; no session reuse.
        succeed(
            f"KILL {kind} '{target_id}'",
            owner if killer == "owner" else ROOT,
            kill_url,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            state = query_state(query_id, owner)
            if state["state"] != "Running":
                break
            time.sleep(0.1)
        assert state["state"] == "Failed", state
        assert state["error"]["code"] == 1043, state  # AbortedQuery
        if id_type == "query":
            assert next_uri is not None
            page_response = requests.get(BASE_URL + next_uri, auth=owner, timeout=30)
            page_response.raise_for_status()
            page = page_response.json()
            assert page["state"] == "Failed", page
            assert page["error"]["code"] == 1043, page
            assert "canceled" in page["error"]["message"], page
    finally:
        if session_id is not None:
            # Cleanup even if an assertion fails; do not use the HTTP kill endpoint.
            command(f"KILL CONNECTION '{session_id}'")
        requests.get(
            f"{BASE_URL}/v1/query/{query_id}/final", auth=owner, timeout=30
        ).raise_for_status()
