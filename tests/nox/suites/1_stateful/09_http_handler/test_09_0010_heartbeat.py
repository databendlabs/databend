import requests
import time

auth = ("root", "")
STICKY_HEADER = "X-DATABEND-STICKY-NODE"

session = {"settings": {"http_handler_result_timeout_secs": "3", "max_threads": "32"}}


def do_query(query, port=8000):
    url = f"http://localhost:{port}/v1/query"
    query_payload = {
        "sql": query,
        "pagination": {
            "wait_time_secs": 2,
            "max_rows_per_page": 4,
            "max_rows_in_buffer": 3,
        },
        "session": session,
    }
    headers = {
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, json=query_payload, auth=auth)
    return response.json()

def do_hb(resps):
    m = {}
    for resp in resps:
        m.setdefault(resp.get("node_id"), []).append(resp.get("id"))
    headers = {
        "Content-Type": "application/json",
    }
    hb_uri = f"http://localhost:8000/v1/session/heartbeat"
    payload = {"node_to_queries": m}
    return requests.post(hb_uri, headers=headers, json=payload, auth=auth).json()

def test_heartbeat():
    resp0 = do_query("select count(*) from system.clusters")
    num_nodes = int(resp0.get("data")[0][0])
    port = 8000 if num_nodes == 1 else 8002

    resp1 = do_query("select * from numbers(100)")
    resp2 = do_query("select * from numbers(100)", port=port)

    for i in range(10):
        response = do_hb([resp1, resp2])
        assert len(response.get("queries_to_remove")) == 0, f"heartbeat {i}: {response}"
        time.sleep(1)

    for i, r in enumerate([resp1, resp2]):
        headers = {STICKY_HEADER: r.get("node_id")}
        next_uri = f"http://localhost:8000/{r.get('next_uri')}?"
        response = requests.get(next_uri, headers=headers, auth=auth)
        assert response.status_code == 200, f"query {i}:{response.status_code} {response.text}"
        response = response.json()
        assert len(response.get("data")) > 0, f"query {i}: {response}"

    response = do_hb([resp0])
    assert len(response.get("queries_to_remove")) == 1, f"resp0: {response}"

    final_uri = f"http://localhost:8000/{resp0.get('final_uri')}?"
    headers = {STICKY_HEADER: resp0.get("node_id")}
    response = requests.get(final_uri, headers=headers, auth=auth)
    assert response.status_code == 200, f"{response}"
    response = response.json()
    assert response["error"] is None
