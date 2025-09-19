import requests
import time
from suites.utils import comparison_output

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


@comparison_output(
    """started query 0
started query 1
sending heartbeat 0
sending heartbeat 1
sending heartbeat 2
sending heartbeat 3
sending heartbeat 4
sending heartbeat 5
sending heartbeat 6
sending heartbeat 7
sending heartbeat 8
sending heartbeat 9
continue fetch 0
continue fetch 1
end
"""
)
def test_heartbeat():
    query_resp = do_query("select count(*) from system.clusters")
    num_nodes = int(query_resp.get("data")[0][0])
    port = 8000 if num_nodes == 1 else 8002

    resp1 = do_query("select * from numbers(100)")
    print("started query 0")
    # print(resp1.get("node_id"), resp1.get("id"))
    resp2 = do_query("select * from numbers(100)", port=port)
    print("started query 1")
    # print(resp1.get("node_id"), resp1.get("id"))
    # print(resp2.get("node_id"), resp2.get("id"))

    url = f"http://localhost:8000/v1/session/heartbeat"
    m = {}
    m.setdefault(resp1.get("node_id"), []).append(resp1.get("id"))
    m.setdefault(resp2.get("node_id"), []).append(resp2.get("id"))
    payload = {"node_to_queries": m}
    headers = {
        "Content-Type": "application/json",
    }
    for i in range(10):
        print(f"sending heartbeat {i}")
        response = requests.post(url, headers=headers, json=payload, auth=auth).json()
        assert len(response.get("queries_to_remove")) == 0
        time.sleep(1)

    for i, r in enumerate([resp1, resp2]):
        print(f"continue fetch {i}")
        headers = {STICKY_HEADER: r.get("node_id")}
        next_uri = f"http://localhost:8000/{r.get('next_uri')}?"
        response = requests.get(next_uri, headers=headers, auth=auth)
        assert response.status_code == 200, f"{response.status_code} {response.text}"
        assert len(response.json().get("data")) > 0
    print("end")
