import requests
import time
from suites.utils import comparison_output

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
heartbeat_url = "http://localhost:8000/v1/session/heartbeat"
auth = ("root", "")
STICKY_HEADER = "X-DATABEND-STICKY-NODE"

# Session settings for heartbeat testing
session = {"settings": {"http_handler_result_timeout_secs": "3", "max_threads": "32"}}


def do_query(query, port=8000):
    """Execute SQL query via HTTP API with specific session settings"""
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
    headers = {"Content-Type": "application/json"}

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
def test_query_heartbeat():
    """Test query heartbeat functionality to keep queries alive"""
    # Check if running in cluster mode to determine port
    cluster_check = do_query("select count(*) from system.clusters")
    num_nodes = int(cluster_check.get("data", [[1]])[0][0])
    port = 8002 if num_nodes > 1 else 8000

    # Start first query
    resp1 = do_query("select * from numbers(100)")
    print("started query 0")

    # Start second query on specified port
    resp2 = do_query("select * from numbers(100)", port=port)
    print("started query 1")

    # Prepare heartbeat payload
    node_to_queries = {}
    node_to_queries.setdefault(resp1.get("node_id"), []).append(resp1.get("id"))
    node_to_queries.setdefault(resp2.get("node_id"), []).append(resp2.get("id"))
    payload = {"node_to_queries": node_to_queries}
    headers = {"Content-Type": "application/json"}

    # Send heartbeats for 10 seconds
    for i in range(10):
        print(f"sending heartbeat {i}")
        response = requests.post(
            heartbeat_url, headers=headers, json=payload, auth=auth
        ).json()
        assert len(response.get("queries_to_remove", [])) == 0
        time.sleep(1)

    # Continue fetching results after heartbeats
    for i, resp in enumerate([resp1, resp2]):
        print(f"continue fetch {i}")
        headers = {STICKY_HEADER: resp.get("node_id")}
        next_uri = f"http://localhost:8000/{resp.get('next_uri')}?"
        response = requests.get(next_uri, headers=headers, auth=auth)
        assert response.status_code == 200
        assert len(response.json().get("data", [])) > 0

    print("end")
