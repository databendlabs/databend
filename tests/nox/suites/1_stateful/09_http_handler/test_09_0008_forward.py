import requests
from suites.utils import comparison_output

auth = ("root", "")
STICKY_HEADER = "X-DATABEND-STICKY-NODE"


def do_query(query, port=8000, session=None, node_id=None, wait=100):
    url = f"http://localhost:{port}/v1/query"
    query_payload = {
        "sql": query,
        "pagination": {"wait_time_secs": wait, "max_rows_per_page": 2},
    }
    if session:
        query_payload["session"] = session
    headers = {
        "Content-Type": "application/json",
    }
    if node_id:
        headers[STICKY_HEADER] = node_id

    response = requests.post(url, headers=headers, json=query_payload, auth=auth)
    return response


def get_txn_state(resp):
    return (
        resp.get("state") == "Succeeded",
        resp.get("session").get("need_sticky"),
        resp.get("session").get("txn_state"),
    )


def test_txn_success():
    # only test under cluster mode
    query_resp = do_query("select count(*) from system.clusters").json()
    num_nodes = int(query_resp.get("data")[0][0])
    if num_nodes == 1:
        return

    resp = do_query("create or replace table t1(a int)").json()
    assert not (resp.get("session").get("need_sticky")), resp

    resp = do_query("begin").json()
    assert resp.get("session").get("need_sticky"), resp
    node_id = resp.get("node_id")
    session = resp.get("session")

    # forward to node 1
    resp = do_query(
        "insert into t1 values (2)", port=8002, session=session, node_id=node_id
    ).json()
    assert get_txn_state(resp) == (True, True, "Active"), resp

    # return need_sticky = false after commit
    resp = do_query("commit").json()
    assert get_txn_state(resp) == (True, False, "AutoCommit"), resp


def test_txn_fail():
    # only test under cluster mode
    query_resp = do_query("select count(*) from system.clusters").json()
    num_nodes = int(query_resp.get("data")[0][0])
    if num_nodes == 1:
        return

    resp = do_query("create or replace table t1(a int)").json()
    assert not (resp.get("session").get("need_sticky")), resp

    resp = do_query("begin").json()
    assert resp.get("session").get("need_sticky"), resp
    node_id = resp.get("node_id")
    session = resp.get("session")

    # fail
    resp = do_query("select 1/0", session=session).json()
    assert get_txn_state(resp) == (False, False, "Fail"), resp

    # fail because wrong node
    resp = do_query("select 1", port=8002, session=session).json()
    session = resp.get("session")
    assert get_txn_state(resp) == (False, False, "Fail"), resp

    # keep fail state until commit/abort
    resp = do_query("select 1", session=session, node_id=node_id).json()
    assert get_txn_state(resp) == (False, False, "Fail"), resp
    session = resp.get("session")

    # return need_sticky = false after commit
    resp = do_query("commit", session=session).json()
    assert get_txn_state(resp) == (True, False, "AutoCommit"), resp

    # return need_sticky = false after abort
    resp = do_query("abort", session=session).json()
    assert get_txn_state(resp) == (True, False, "AutoCommit"), resp


def test_query():
    # only test under cluster mode
    query_resp = do_query("select count(*) from system.clusters").json()
    num_nodes = int(query_resp.get("data")[0][0])
    if num_nodes == 1:
        return

    """each query is sticky"""
    # send SQL to node-1
    initial_resp = do_query("select * from numbers(10)").json()
    assert len(initial_resp.get("data")) == 2

    # get page from node-2 without header
    next_uri = initial_resp.get("next_uri")
    next_uri = f"http://localhost:8002/{next_uri}?"
    resp = requests.get(next_uri, auth=auth)
    assert resp.status_code == 404

    # get page from node-2 by forward
    node_id = initial_resp.get("node_id")
    headers = {
        STICKY_HEADER: node_id,
    }
    resp = requests.get(next_uri, auth=auth, headers=headers)
    assert resp.status_code == 200, resp.text
    assert len(resp.json().get("data")) == 2

    # error: query not exists
    resp = requests.get(
        "http://localhost:8002/v1/query/an_query_id/page/0", auth=auth, headers=headers
    )
    assert resp.status_code == 404, resp.text

    # error: node not exists
    headers = {
        STICKY_HEADER: "xxx",
    }
    resp = requests.get(next_uri, auth=auth, headers=headers)
    assert resp.status_code == 400, resp.text


def test_initial_response():
    sql = "select * from numbers(1000000000000) ignore_result"
    resp = do_query(sql, wait=1).json()
    assert not (resp.get("session").get("need_sticky")), resp
