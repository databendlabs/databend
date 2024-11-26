#!/usr/bin/env python3
import time
import logging
import requests
from http.cookiejar import Cookie
from requests.cookies import RequestsCookieJar

auth = ("root", "")
logging.basicConfig(level=logging.ERROR, format="%(asctime)s %(levelname)s %(message)s")


class GlobalCookieJar(RequestsCookieJar):
    def __init__(self):
        super().__init__()

    def set_cookie(self, cookie: Cookie, *args, **kwargs):
        cookie.domain = ""
        cookie.path = "/"
        super().set_cookie(cookie, *args, **kwargs)

    def get_dict(self, domain=None, path=None):
        # 忽略 domain 和 path 参数，返回所有 Cookie
        return {cookie.name: cookie.value for cookie in self}


def do_query(session_client, query, session_state=None):
    url = f"http://localhost:8000/v1/query"
    query_payload = {
        "sql": query,
        "pagination": {"wait_time_secs": 100, "max_rows_per_page": 2},
    }
    if session_state:
        query_payload["session"] = session_state
    headers = {
        "Content-Type": "application/json",
    }

    response = session_client.post(url, headers=headers, json=query_payload, auth=auth)
    return response


def test_simple():
    client = requests.session()
    client.cookies = GlobalCookieJar()
    client.cookies.set("cookie_enabled", "true")

    resp = do_query(client, "select 1")
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"] == [["1"]], resp.text
    sid = client.cookies.get("session_id")
    # print(sid)

    last_access_time1 = int(client.cookies.get("last_access_time"))
    # print(last_access_time1)
    assert time.time() - 10 < last_access_time1 < time.time()

    time.sleep(1.5)

    resp = do_query(client, "select 1")
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"] == [["1"]], resp.text
    sid2 = client.cookies.get("session_id")
    last_access_time2 = int(client.cookies.get("last_access_time"))
    assert sid2 == sid
    assert last_access_time1 < last_access_time2 < time.time()


def test_temp_table():
    client = requests.session()
    client.cookies = GlobalCookieJar()
    client.cookies.set("cookie_enabled", "true")

    resp = do_query(client, "create temp table t1(a int)")
    assert resp.status_code == 200, resp.text
    session_state = resp.json()["session"]
    assert session_state["need_sticky"], resp.text
    assert session_state["need_keep_alive"]

    resp = do_query(client, "insert into t1 values (3), (4)", session_state)
    assert resp.status_code == 200, resp.text
    session_state = resp.json()["session"]
    assert session_state["need_sticky"], resp.text
    assert session_state["need_keep_alive"]

    resp = do_query(client, "select * from t1", session_state)
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"] == [["3"], ["4"]]
    session_state = resp.json()["session"]
    assert session_state["need_sticky"], resp.text
    assert session_state["need_keep_alive"]

    resp = do_query(client, "drop table t1", session_state)
    assert resp.status_code == 200, resp.text
    session_state = resp.json()["session"]
    assert not session_state["need_sticky"]
    assert not session_state["need_keep_alive"]


def main():
    test_simple()
    test_temp_table()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
