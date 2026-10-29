import base64
import json
import time
from uuid import UUID

import pytest
import requests

from suites.utils import comparison_output

HEADER_SESSION = "X-DATABEND-SESSION"
HEADER_SESSION_ID = "X-DATABEND-SESSION-ID"
HEADER_CAPS = "X-DATABEND-CLIENT-CAPS"
query_url = "http://localhost:8000/v1/query"
login_url = "http://localhost:8000/v1/session/login"
logout_url = "http://localhost:8000/v1/session/logout"
auth = ("root", "")


def encode_session_header(value):
    return base64.urlsafe_b64encode(json.dumps(value).encode("utf-8")).decode("ascii")


def check_session_response(response):
    assert response.status_code == 200, response.text
    encoded = response.headers.get(HEADER_SESSION)
    assert encoded, response.headers
    session = json.loads(base64.urlsafe_b64decode(encoded))
    assert str(UUID(session["id"])) == session["id"], session
    assert isinstance(session["last_refresh_time"], int), session
    assert session["last_refresh_time"] > 0, session
    assert response.headers[HEADER_SESSION_ID] == session["id"], response.headers
    assert "Set-Cookie" not in response.headers, response.headers
    assert not response.cookies, response.cookies
    return session


class Client:
    def __init__(self):
        self.client = requests.Session()
        self.client.auth = auth
        self.client.headers[HEADER_CAPS] = "session_header"
        self.session_header_json = None

    def request(self, method, url, **kwargs):
        response = self.client.request(method, url, timeout=30, **kwargs)
        session = check_session_response(response)
        if self.session_header_json is not None:
            assert session["id"] == self.session_header_json["id"], session
        self.session_header_json = session
        # Echo the server's opaque header on every subsequent request.
        self.client.headers[HEADER_SESSION] = response.headers[HEADER_SESSION]
        return response.json()

    def login(self):
        body = self.request("POST", login_url, json={})
        assert body["session_id"] == self.session_header_json["id"], body
        return body

    def do_logout(self):
        body = self.request("POST", logout_url)
        assert body["error"] is None, body
        return body

    def do_query(self, query, session_state=None, pagination=None):
        payload = {
            "sql": query,
            "pagination": pagination or {"wait_time_secs": 10},
        }
        if session_state is not None:
            payload["session"] = session_state
        body = self.request("POST", query_url, json=payload)
        assert body["error"] is None, body
        assert body["session_id"] == self.session_header_json["id"], body
        return body


@pytest.fixture
def client():
    client = Client()
    try:
        yield client
    finally:
        client.client.close()


@pytest.mark.parametrize("url", [login_url, query_url], ids=["login", "query"])
@pytest.mark.parametrize("header", [None, ""], ids=["missing", "empty"])
def test_new_session(client, url, header):
    headers = {} if header is None else {HEADER_SESSION: header}
    payload = {} if url == login_url else {"sql": "select 1"}
    started = int(time.time())
    body = client.request("POST", url, headers=headers, json=payload)
    session = client.session_header_json
    assert body["session_id"] == session["id"], body
    assert started <= session["last_refresh_time"] <= int(time.time()), session
    if url == query_url:
        assert body["error"] is None, body
        assert body["data"] == [["1"]], body


@pytest.mark.parametrize(
    "caps",
    [
        "session_header",
        "SESSION_HEADER",
        "unknown; Session_Header ; ; unknown",
        "session_header;session_header",
    ],
)
def test_session_header_capabilities(client, caps):
    client.client.headers[HEADER_CAPS] = caps
    assert client.do_query("select 1")["data"] == [["1"]]
    session = client.session_header_json.copy()
    assert client.do_query("select 2")["data"] == [["2"]]
    assert client.session_header_json == session


@pytest.mark.parametrize("trailing_whitespace", [False, True])
def test_session_header_round_trip(client, trailing_whitespace):
    client.login()
    session = client.session_header_json.copy()
    if trailing_whitespace:
        client.client.headers[HEADER_SESSION] += "   "
    assert client.do_query("select 1")["data"] == [["1"]]
    assert client.session_header_json == session
    client.do_logout()
    assert client.session_header_json == session


def test_session_header_capability_is_per_request(client):
    client.login()
    session = client.session_header_json.copy()
    # The login capability is not remembered when a later request omits it.
    response = client.client.post(
        query_url,
        headers={HEADER_CAPS: None},
        json={"sql": "select 1", "pagination": {"wait_time_secs": 10}},
        timeout=30,
    )
    assert response.status_code == 200, response.text
    assert HEADER_SESSION not in response.headers, response.headers
    assert HEADER_SESSION_ID not in response.headers, response.headers
    assert "Set-Cookie" not in response.headers, response.headers
    body = response.json()
    assert body["error"] is None, body
    assert body["data"] == [["1"]], body
    assert body["session_id"] == "", body
    assert client.do_query("select 2")["data"] == [["2"]]
    assert client.session_header_json == session


def test_session_header_on_query_error(client):
    client.login()
    session = client.session_header_json.copy()
    body = client.request(
        "POST", query_url, json={"sql": "SELECT * FROM session_header_missing_table"}
    )
    assert body["state"] == "Failed", body
    assert "Unknown table" in body["error"]["message"], body
    assert client.session_header_json == session
    assert client.do_query("select 1")["data"] == [["1"]]


def test_session_refresh(client):
    client.login()
    session = client.session_header_json.copy()
    # Force refresh without sleeping for the server's refresh interval (<= 300s).
    stale = {**session, "last_refresh_time": int(time.time()) - 600}
    client.client.headers[HEADER_SESSION] = encode_session_header(stale)
    started = int(time.time())
    assert client.do_query("select 1")["data"] == [["1"]]
    refreshed = client.session_header_json.copy()
    assert refreshed["id"] == session["id"]
    assert started <= refreshed["last_refresh_time"] <= int(time.time()), refreshed
    assert refreshed["last_refresh_time"] > stale["last_refresh_time"]
    client.do_query("select 2")
    assert client.session_header_json == refreshed


def test_session_header_precedes_cookies(client):
    client.login()
    session = client.session_header_json.copy()
    client.client.headers[HEADER_CAPS] = "session_cookie;session_header"
    # A conflicting cookie must not override the custom-header session.
    client.client.cookies.set("session_id", "cookie-session")
    client.client.cookies.set("last_refresh_time", "not-a-timestamp")
    client.client.cookies.set("cookie_enabled", "true")
    assert client.do_query("select 1")["data"] == [["1"]]
    assert client.session_header_json == session


def test_session_pagination(client):
    body = client.do_query(
        "select number from numbers(5) order by number",
        pagination={"wait_time_secs": 10, "max_rows_per_page": 2},
    )
    session = client.session_header_json.copy()
    data = body["data"]
    final_uri = body["final_uri"]
    next_uri = body["next_uri"]
    assert next_uri and next_uri != final_uri, body
    # Bound the loop so a regressed next_uri cannot hang the suite.
    for _ in range(10):
        if not next_uri or next_uri == final_uri:
            break
        body = client.request("GET", f"http://localhost:8000{next_uri}")
        assert body["error"] is None, body
        assert client.session_header_json == session
        data.extend(body["data"])
        next_uri = body["next_uri"]
    else:
        pytest.fail("pagination did not finish within 10 pages")
    assert data == [[str(i)] for i in range(5)]
    body = client.request("GET", f"http://localhost:8000{final_uri}")
    assert body["error"] is None, body
    assert body["next_uri"] is None, body
    assert client.session_header_json == session


def test_session(client):
    client.login()
    body = client.do_query("select 1")
    assert body["data"] == [["1"]], body
    assert not body["session"]["need_sticky"], body
    assert not body["session"]["need_keep_alive"], body

    body = client.do_query("CREATE TEMP TABLE session_header_temp(c1 int)")
    state = body["session"]
    try:
        assert state["need_sticky"], body
        assert state["need_keep_alive"], body
        body = client.do_query("INSERT INTO session_header_temp VALUES (42)", state)
        state = body["session"]
        body = client.do_query("SELECT * FROM session_header_temp", state)
        state = body["session"]
        assert body["data"] == [["42"]], body
        assert state["need_sticky"], body
        assert state["need_keep_alive"], body
        # Another client must not see the temporary table, even with its SQL session state.
        with requests.Session() as other:
            response = other.post(
                query_url,
                auth=auth,
                headers={HEADER_CAPS: "session_header"},
                json={"sql": "SELECT * FROM session_header_temp", "session": state},
                timeout=30,
            )
            other_session = check_session_response(response)
            assert other_session["id"] != client.session_header_json["id"]
            # Replaying another client's SQL state must not attach its temp tables.
            error = response.json()["error"]
            assert error["code"] == 5110, response.text
            assert "temporary tables in session" in error["message"], error
            assert "expired" in error["message"], error
        body = client.do_query("SELECT * FROM session_header_temp", state)
        state = body["session"]
        assert body["data"] == [["42"]], body
    finally:
        body = client.do_query("DROP TABLE session_header_temp", state)
    assert not body["session"]["need_sticky"], body
    assert not body["session"]["need_keep_alive"], body
    client.do_logout()


@pytest.mark.parametrize("caps", [None, "", "unknown", "session_header_v2"])
def test_no_session(caps):
    # Without the exact capability, even an invalid session header is ignored.
    headers = {HEADER_SESSION: "not-base64!"}
    if caps is not None:
        headers[HEADER_CAPS] = caps
    with requests.Session() as client:
        response = client.post(
            query_url,
            auth=auth,
            headers=headers,
            json={
                "sql": "select number from numbers(5) order by number",
                "pagination": {"wait_time_secs": 10, "max_rows_per_page": 2},
            },
            timeout=30,
        )
        assert response.status_code == 200, response.text
        assert HEADER_SESSION not in response.headers, response.headers
        assert HEADER_SESSION_ID not in response.headers, response.headers
        assert not response.cookies, response.cookies
        body = response.json()
        assert body["error"] is None, body
        assert body["session_id"] == "", body
        final_uri = body["final_uri"]
        next_uri = body["next_uri"]
        assert next_uri and next_uri != final_uri, body
        response = client.get(
            f"http://localhost:8000{next_uri}", auth=auth, headers=headers, timeout=30
        )
        assert response.status_code == 200, response.text
        assert HEADER_SESSION not in response.headers, response.headers
        body = response.json()
        assert body["error"] is None, body
        assert body["data"] == [["2"], ["3"]], body
        response = client.get(
            f"http://localhost:8000{final_uri}", auth=auth, headers=headers, timeout=30
        )
        assert response.status_code == 200, response.text
        assert HEADER_SESSION not in response.headers, response.headers
        assert response.json()["error"] is None, response.text


@pytest.mark.parametrize("url", [login_url, query_url], ids=["login", "query"])
@pytest.mark.parametrize(
    "header, message",
    [
        ("not-base64!", "base64 decode error"),
        (base64.urlsafe_b64encode(b"not json").decode("ascii"), "decode error"),
        ("{", "JSON decode error"),
        (encode_session_header({}), "missing field"),
        (encode_session_header({"id": "test"}), "last_refresh_time"),
        (
            encode_session_header({"id": "test", "last_refresh_time": "invalid"}),
            "invalid type",
        ),
        (
            encode_session_header({"id": "test", "last_refresh_time": -1}),
            "invalid value",
        ),
    ],
    ids=[
        "base64",
        "json",
        "raw-json",
        "missing-id",
        "missing-time",
        "time-type",
        "negative-time",
    ],
)
def test_invalid_session_header(client, url, header, message):
    response = client.client.post(
        url,
        headers={HEADER_SESSION: header},
        json={} if url == login_url else {"sql": "select 1"},
        timeout=30,
    )
    # Decode failures currently map to Internal error; assert the diagnostic too,
    # so an unrelated server error cannot make this test pass.
    assert response.status_code == 500, response.text
    error = response.json()["error"]
    assert HEADER_SESSION in error["message"], error
    assert message in error["message"], error
    assert HEADER_SESSION not in response.headers, response.headers
    assert not response.cookies, response.cookies


HEADER_SESSION_ID_V = "101010"


def do_query_from_worksheet(client, sql, sid=HEADER_SESSION_ID_V, new_session=False):
    internal = None if new_session else "{}"
    payload = {
        "sql": sql,
        "pagination": {"max_rows_per_page": 2, "wait_time_secs": 10},
        "session": {"internal": internal},
    }
    resp = client.post(
        query_url,
        auth=auth,
        headers={
            "Content-Type": "application/json",
            "USER-AGENT": "worksheet",
            HEADER_SESSION_ID: sid,
        },
        json=payload,
    )
    return resp.json()


@comparison_output("")
def test_worksheet_session():
    client = requests.session()
    resp = do_query_from_worksheet(client, "select * from numbers(100)")
    next_uri = resp.get("next_uri")

    resp = client.get(
        f"http://localhost:8000/{next_uri}",
        auth=auth,
        headers={"USER-AGENT": "worksheet", HEADER_SESSION_ID: HEADER_SESSION_ID_V},
    )
    resp = resp.json()
    assert len(resp["data"]) == 2, resp

    resp = do_query_from_worksheet(
        client, "create or replace temp table t09_0007(a int)"
    )
    assert resp["state"] == "Succeeded", resp
    resp = do_query_from_worksheet(client, "insert into t09_0007 values (1)")
    assert resp["state"] == "Succeeded", resp
    resp = do_query_from_worksheet(client, "select * from t09_0007")
    assert resp["data"] == [["1"]], resp

    resp = do_query_from_worksheet(client, "select * from t09_0007", new_session=True)
    assert "Unknown table" in resp["error"]["message"], resp

    resp = do_query_from_worksheet(
        client, "select * from numbers(10) ", new_session=True
    )
    assert len(resp["data"]) == 2, resp
