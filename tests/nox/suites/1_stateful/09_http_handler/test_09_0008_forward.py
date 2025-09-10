import requests
from suites.utils import comparison_output

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
auth = ("root", "")
STICKY_HEADER = "X-DATABEND-STICKY-NODE"


def do_query(query, port=8000, session=None, node_id=None, wait=100):
    """Execute SQL query via HTTP API with optional session and node forwarding"""
    url = f"http://localhost:{port}/v1/query"
    query_payload = {
        "sql": query,
        "pagination": {"wait_time_secs": wait, "max_rows_per_page": 2},
    }
    if session:
        query_payload["session"] = session

    headers = {"Content-Type": "application/json"}
    if node_id:
        headers[STICKY_HEADER] = node_id

    response = requests.post(url, headers=headers, json=query_payload, auth=auth)
    return response.json()


def get_txn_state(resp):
    """Extract transaction state from response"""
    return (
        resp.get("state") == "Succeeded",
        resp.get("session", {}).get("need_sticky", False),
        resp.get("session", {}).get("txn_state", ""),
    )


@comparison_output("")
def test_forward_transactions():
    """Test transaction forwarding and sticky sessions in cluster mode"""
    # Check if running in cluster mode (more than one node)
    cluster_check = do_query("select count(*) from system.clusters")
    num_nodes = int(cluster_check.get("data", [[1]])[0][0])
    if num_nodes == 1:
        return

    # Test initial response without sticky session
    sql = "select * from numbers(1000000000000) ignore_result"
    resp = do_query(sql, wait=1)
    assert not (resp.get("session", {}).get("need_sticky", False)), resp

    # Test transaction success with forwarding
    resp = do_query("create or replace table t1(a int)")
    assert not (resp.get("session", {}).get("need_sticky", False)), resp

    resp = do_query("begin")
    assert resp.get("session", {}).get("need_sticky", False), resp
    node_id = resp.get("node_id")
    session = resp.get("session")

    # Forward to node 1 (port 8002)
    resp = do_query(
        "insert into t1 values (2)", port=8002, session=session, node_id=node_id
    )
    assert get_txn_state(resp) == (True, True, "Active"), resp

    # Return need_sticky = false after commit
    resp = do_query("commit", session=session)
    assert get_txn_state(resp) == (True, False, "AutoCommit"), resp

    # Test transaction failure scenarios
    resp = do_query("create or replace table t1(a int)")
    assert not (resp.get("session", {}).get("need_sticky", False)), resp

    resp = do_query("begin")
    assert resp.get("session", {}).get("need_sticky", False), resp
    node_id = resp.get("node_id")
    session = resp.get("session")

    # Fail with division by zero
    resp = do_query("select 1/0", session=session)
    assert get_txn_state(resp) == (False, False, "Fail"), resp

    # Fail because wrong node (no sticky header)
    resp = do_query("select 1", port=8002, session=session)
    session = resp.get("session")
    assert get_txn_state(resp) == (False, False, "Fail"), resp

    # Keep fail state until commit/abort
    resp = do_query("select 1", session=session, node_id=node_id)
    assert get_txn_state(resp) == (False, False, "Fail"), resp
    session = resp.get("session")

    # Return need_sticky = false after commit
    resp = do_query("commit", session=session)
    assert get_txn_state(resp) == (True, False, "AutoCommit"), resp

    # Return need_sticky = false after abort
    resp = do_query("abort", session=session)
    assert get_txn_state(resp) == (True, False, "AutoCommit"), resp

    # Test query pagination with sticky sessions
    initial_resp = do_query("select * from numbers(10)")
    assert len(initial_resp.get("data", [])) == 2

    # Get page from node-2 without header should fail
    next_uri = initial_resp.get("next_uri")
    next_uri = f"http://localhost:8002/{next_uri}?"
    resp = requests.get(next_uri, auth=auth)
    assert resp.status_code == 404

    # Get page from node-2 with sticky header should succeed
    node_id = initial_resp.get("node_id")
    headers = {STICKY_HEADER: node_id}
    resp = requests.get(next_uri, auth=auth, headers=headers)
    assert resp.status_code == 200
    assert len(resp.json().get("data", [])) == 2

    # Error: query not exists
    resp = requests.get(
        "http://localhost:8002/v1/query/an_query_id/page/0", auth=auth, headers=headers
    )
    assert resp.status_code == 404

    # Error: node not exists
    headers = {STICKY_HEADER: "xxx"}
    resp = requests.get(next_uri, auth=auth, headers=headers)
    assert resp.status_code == 400
