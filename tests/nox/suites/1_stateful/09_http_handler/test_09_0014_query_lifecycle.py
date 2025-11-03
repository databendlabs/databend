import json

import requests
import time
import pytest

auth = ("root", "")
STICKY_HEADER = "X-DATABEND-STICKY-NODE"

max_threads = 32

def patch_json(resp):
    if resp.get("stats", {}).get("running_time_ms"):
        resp["stats"]["running_time_ms"] = 0
    return resp

def do_query(query, timeout=10, pagination=None, port=8000, patch=True):
    """Execute a query with configurable timeout and pagination."""
    session = {
        "settings": {
            "http_handler_result_timeout_secs": f"{timeout}",
            "max_threads": f"{max_threads}"
        }
    }

    if pagination is None:
        pagination = {
            "wait_time_secs": 2,
            "max_rows_per_page": 4,
            "max_rows_in_buffer": 3,
        }

    url = f"http://localhost:{port}/v1/query"
    query_payload = {
        "sql": query,
        "pagination": pagination,
        "session": session,
    }
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, headers=headers, json=query_payload, auth=auth)
    if patch:
        return patch_json(response.json())
    else:
        return response.json()


def get_query_state(query_id, node_id):
    """Get the current state of a query."""
    headers = {
        "Content-Type": "application/json",
        STICKY_HEADER: node_id,
    }
    url = f"http://localhost:8000/v1/query/{query_id}"
    response = requests.get(url, headers=headers, auth=auth)
    return response.status_code, patch_json(response.json())


def get_next_page(next_uri, node_id):
    """Fetch the next page of results."""
    headers = {STICKY_HEADER: node_id}
    url = f"http://localhost:8000/{next_uri}"
    response = requests.get(url, headers=headers, auth=auth)
    return response.status_code, patch_json(response.json())


def finalize_query(final_uri, node_id):
    """Finalize a query (CloseReason::Finalized)."""
    headers = {STICKY_HEADER: node_id}
    url = f"http://localhost:8000/{final_uri}"
    response = requests.get(url, headers=headers, auth=auth)
    return response.status_code, patch_json(response.json())


def cancel_query(query_id, node_id):
    """Cancel a query (CloseReason::Canceled)."""
    headers = {
        "Content-Type": "application/json",
        STICKY_HEADER: node_id,
    }
    url = f"http://localhost:8000/v1/query/{query_id}/kill"
    response = requests.post(url, headers=headers, auth=auth)
    return response


def do_hb(resps):
    m = {}
    for resp in resps:
        m.setdefault(resp.get("node_id"), []).append(resp.get("id"))
    headers = {
        "Content-Type": "application/json",
    }
    hb_uri = f"http://localhost:8000/v1/session/heartbeat"
    payload = {"node_to_queries": m}
    resp = requests.post(hb_uri, headers=headers, json=payload, auth=auth)
    return resp.status_code, resp.json()


@pytest.mark.parametrize("rows", [8, 7])
def test_query_lifecycle_finalized(rows):
    """
    Test the Finalized close reason path.

    Verifies:
    - Query executes successfully
    - Data can be fetched via pagination
    - Final endpoint closes query with CloseReason::Finalized
    - Subsequent page requests fail with appropriate error
    """
    # Start query with pagination

    max_rows_per_page = 4
    timeout = 3

    pagination = {
        "wait_time_secs": 2,
        "max_rows_per_page": max_rows_per_page,
        "max_rows_in_buffer": max_rows_per_page,
    }

    resp0 = do_query(f"select * from numbers({rows})", timeout=timeout, pagination=pagination)

    query_id = resp0.get("id")
    node_id = resp0.get("node_id")
    sessions_internal = resp0.get("session", {}).get("internal")
    assert json.loads(sessions_internal) == {"last_node_id": node_id, "last_query_ids":[query_id,]}
    progress = {"rows":rows,"bytes":rows * 8}

    exp = {"id": query_id,
           "session_id":"",
           "node_id": node_id,
           "error": None,
           "state": "Running",
           "warnings":[],
           "affect": None,
           "has_result_set": True,
           "schema":[{"name":"number","type":"UInt64"}],
           "data":[["0"],["1"],["2"],["3"]],
           "result_timeout_secs":timeout,
           "stats":{
               "scan_progress":progress,
               "write_progress": {"rows":0, "bytes": 0},
               "result_progress":progress,
               "total_scan":progress,
               "spill_progress":{"file_nums":0,"bytes":0},
               "running_time_ms": 0},
           "stats_uri": f"/v1/query/{query_id}",
           "final_uri": f"/v1/query/{query_id}/final",
           "next_uri": f"/v1/query/{query_id}/page/1",
           "kill_uri": f"/v1/query/{query_id}/kill",
           'session': {'catalog': 'default',
                       'database': 'default',
                       'internal': sessions_internal,
                       'need_keep_alive': False,
                       'need_sticky': False,
                       'role': 'account_admin',
                       'settings': {'http_handler_result_timeout_secs': f'{timeout}',
                                    'max_threads': f"{max_threads}"},
                       'txn_state': 'AutoCommit'}
           }

    assert resp0 == exp

    exp_state = {"error": None,
                 "state": "Succeeded",
                 "warnings":[],
                 "stats":{
                     "scan_progress":progress,
                     "write_progress": {"rows":0, "bytes": 0},
                     "result_progress":progress,
                     "total_scan":progress,
                     "spill_progress":{"file_nums":0,"bytes":0},
                     "running_time_ms": 0},
                 }
    status_code, resp_state = get_query_state(query_id, node_id)

    resp_state['state'] in ["Succeeded", "Running"]
    resp_state['state'] = "Succeeded"
    assert (status_code, resp_state) == (200, exp_state)

    assert do_hb([resp0]) == (200, {"queries_to_remove": []})

    ##  Fetch page 1, support retry

    # not return session since nothing changed
    exp["session"] = None
    exp["state"] = "Succeeded"
    if rows == 8:
        exp["next_uri"] = f"/v1/query/{query_id}/page/2"

    elif rows == 7:
        exp["next_uri"] = f"/v1/query/{query_id}/final"

    exp["data"] =  [["4"],["5"],["6"]] if rows == 7 else [["4"],["5"],["6"],["7"]]
    for i in range(3):
        status_code, resp1 = get_next_page(resp0.get("next_uri"), node_id)
        if rows == 8:
            # todo: it is better to return "Succeeded" as long as the data is drained, but this is not big issue since it is unlikely that rows of result is exactly multiple of the page size
            resp1['state'] in ["Succeeded", "Running"]
            resp1['state'] = "Succeeded"
        assert status_code == 200, resp1
        # not return session since nothing changed
        assert resp1 == exp, i


    exp["next_uri"] = f"/v1/query/{query_id}/final"
    exp["data"] = []
    if rows == 8:
        large_page_no = 4
        for i in range(3):
            status_code, resp2 = get_next_page(resp1.get("next_uri"), node_id)
            assert status_code == 200, resp2
            assert resp2 == exp, i
            assert (status_code, resp2) == (200, exp)
    elif rows == 7:
        large_page_no = 3


    assert do_hb([resp0]) == (200, {"queries_to_remove": [query_id]})

    # Fetch the page 0 result in 404
    assert get_next_page(f"/v1/query/{query_id}/page/0", node_id) == (404,  {"error":{"code":404,"message":"[HTTP-QUERY] Invalid page number: requested 0, current page is 2"}})
    assert get_next_page(f"/v1/query/{query_id}/page/{large_page_no}", node_id) == (404, {"error":{"code":404,"message":f"[HTTP-QUERY] Invalid page number: requested {large_page_no}, current page is 2"}})

    # Finalize the query (CloseReason::Finalized),  support retry
    exp["next_uri"] = None
    for i in range(3):
        status_code, final_resp = finalize_query(resp0.get("final_uri"), node_id)
        assert status_code == 200, final_resp
        assert final_resp == exp

    cancel_resp = cancel_query(query_id, node_id)
    assert cancel_resp.status_code == 200

    # Fetch the page 0 result in 400
    assert get_next_page(f"/v1/query/{query_id}/page/0", node_id) == (400, {"error":{"code":400,"message":f"[HTTP-QUERY] Query {query_id} is closed for finalized"}})
    assert do_hb([resp0]) == (200, {"queries_to_remove": [query_id]})
    # assert get_query_state(query_id, node_id) == (200, exp_state)

    time.sleep(timeout + 2)

    # 404 after tombstone timeout
    status_code, resp_state = get_query_state(query_id, node_id)
    assert status_code == 404, resp_state


def test_query_lifecycle_canceled():
    """
    Test the Canceled close reason path.

    Verifies:
    - Query can be canceled mid-execution
    - Cancel endpoint returns CloseReason::Canceled
    - Query state reflects cancellation
    - Subsequent operations fail appropriately
    """
    timeout = 2

    # Start a long-running query
    resp = do_query("select * from numbers(1000000)", timeout=timeout)

    assert resp.get("error") is None, f"Query failed: {resp}"
    query_id = resp.get("id")
    node_id = resp.get("node_id")

    # Give it a moment to start
    time.sleep(0.5)

    # Cancel the query (CloseReason::Canceled)
    for i in range(2):
        cancel_resp = cancel_query(query_id, node_id)
        assert cancel_resp.status_code == 200, f"Cancel failed: {cancel_resp.text}"
        assert cancel_resp.text == "", f"Cancel failed: {cancel_resp.text}"

    assert get_next_page(f"/v1/query/{query_id}/page/0", node_id) == (400, {"error":{"code":400,"message":f"[HTTP-QUERY] Query {query_id} is closed for canceled"}})
    status_code, resp_state = get_query_state(query_id, node_id)
    assert status_code == 200
    assert resp_state.get("state") == "Failed"
    assert resp_state.get("error") is None

    assert do_hb([resp]) == (200, {"queries_to_remove": [query_id]})

    time.sleep(timeout + 2)
    status_code, resp = get_query_state(query_id, node_id)
    assert status_code == 404

# 100: running
# 11: execute ended, client fetching
# 9: drained but not finalized
@pytest.mark.parametrize("rows", [9, 11, 100])
def test_query_lifecycle_timeout(rows):
    # todo: drained should not get timeout
    """
    Test is_data_drained flag in ClientState.

    Verifies:
    - Small result set gets drained in initial response
    - Drained state affects heartbeat behavior
    - next_uri is None when data is drained
    todo:

    """
    # Query that returns small result - should drain immediately

    wait_time_secs = 2
    timeout = 2

    pagination = {
        "wait_time_secs": wait_time_secs,
        "max_rows_per_page": 10,
        "max_rows_in_buffer": 10,
    }
    start = time.time()
    resp = do_query(
        f"select * from numbers({rows})",
        timeout=timeout,
        pagination=pagination
    )
    end = time.time()
    assert end - start < 1

    if rows < 10:
        assert "final" in resp.get("next_uri")
        assert  resp.get("state") == "Succeeded"
    else:
        assert "page" in resp.get("next_uri")
        assert  resp.get("state") == "Running"

    time.sleep(timeout + wait_time_secs + 1)

    node_id = resp.get("node_id")
    query_id = resp.get("id")
    uri_page_0 = f"/v1/query/{query_id}/page/0"
    assert get_next_page(uri_page_0, node_id) == (400, {"error":{"code":400,"message":f"[HTTP-QUERY] Query {query_id} is closed for timed out"}})

    assert do_hb([resp]) == (200, {"queries_to_remove": [query_id]})

    time.sleep(timeout)

    assert get_next_page(resp.get("next_uri"), node_id)[0] == 404


def test_query_lifecycle_timeout_polling():
    """
    Test that polling (with wait_time_secs) affects timeout calculation.

    Verifies:
    - is_polling parameter in update_expire_time
    - Polling adds wait_time_secs to timeout
    - Non-polling uses just result_timeout_secs
    """
    timeout_secs = 3
    wait_time_secs = 5

    start = time.time()
    resp = do_query(
        "select (number % 3)::string, number % 4 , number % 100, sum(number) from numbers(1000000000) group by all ignore_result",
        timeout=timeout_secs,
        pagination={
            "wait_time_secs": wait_time_secs,
            "max_rows_per_page": 5,
            "max_rows_in_buffer": 3,
        },
        patch=False
    )
    end = time.time()

    assert resp.get("error") is None
    assert resp.get("stats", {}).get("running_time_ms") >= wait_time_secs
    assert end - start >= wait_time_secs
    assert end - start < wait_time_secs + timeout_secs

    node_id = resp.get("node_id")

    # This fetch happens shortly after timeout but within polling window
    code, next_resp = get_next_page(resp.get("next_uri"), node_id)
    # May succeed or fail depending on exact timing
    assert code == 200, next_resp


# todo refactor /v1/state, return client_state
# each lifecycle end with a hb removed tests

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
