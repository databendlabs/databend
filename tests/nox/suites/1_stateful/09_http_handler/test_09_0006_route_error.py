import requests
import random
import json
from suites.utils import comparison_output

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
auth = ("root", "")


def execute_query(sql, query_id):
    """Execute SQL query via HTTP API with specific query ID"""
    headers = {"Content-Type": "application/json", "x-databend-query-id": query_id}
    payload = {"sql": sql}
    response = requests.post(query_url, auth=auth, headers=headers, json=payload)
    return response.json()


def get_query_page(query_id, page_number, node_id=None):
    """Get a specific page of query results with optional node ID header"""
    page_url = f"http://localhost:8000/v1/query/{query_id}/page/{page_number}"
    headers = {}
    if node_id:
        headers["x-databend-node-id"] = node_id
    response = requests.get(page_url, auth=auth, headers=headers)
    return response.status_code, response.text


def kill_query(query_id, node_id=None):
    """Kill a query with optional node ID header"""
    kill_url = f"http://localhost:8000/v1/query/{query_id}/kill"
    headers = {}
    if node_id:
        headers["x-databend-node-id"] = node_id
    response = requests.get(kill_url, auth=auth, headers=headers)
    return response.status_code, response.text


def get_query_final(query_id, node_id=None):
    """Get final result of a query with optional node ID header"""
    final_url = f"http://localhost:8000/v1/query/{query_id}/final"
    headers = {}
    if node_id:
        headers["x-databend-node-id"] = node_id
    response = requests.get(final_url, auth=auth, headers=headers)
    return response.status_code, response.text


@comparison_output(
    """# error
## page
{"error":{"code":404,"message":"Routing error: query QID should be on server XXX, but current server is NODE, which started ... ago)"}}
## kill
{"error":{"code":404,"message":"Routing error: query QID should be on server XXX, but current server is NODE, which started ... ago)"}}
## final
{"error":{"code":404,"message":"Routing error: query QID should be on server XXX, but current server is NODE, which started ... ago)"}}

# ok
## page
[["1"]]
## kill
200
## final
null"""
)
def test_route_error():
    # Generate random query ID
    qid = f"my_query_for_route_{random.randint(1000, 9999)}"

    # Execute a simple query to get the node ID
    query_result = execute_query("select 1;", qid)
    session_internal = query_result.get("session", {}).get("internal", "{}")
    node_id = json.loads(session_internal).get("last_node_id")

    print("# error")
    print("## page")
    # Try to get page with wrong node ID
    page_status, page_text = get_query_page(qid, 0, "XXX")
    # Replace actual IDs with placeholders for consistent output
    page_text = page_text.replace(qid, "QID").replace(node_id, "NODE")
    # Remove timestamp details for consistent output
    import re

    page_text = re.sub(r"at.*secs", "...", page_text)
    print(page_text)

    print("## kill")
    # Try to kill with wrong node ID
    kill_status, kill_text = kill_query(qid, "XXX")
    kill_text = kill_text.replace(qid, "QID").replace(node_id, "NODE")
    kill_text = re.sub(r"at.*secs", "...", kill_text)
    print(kill_text)

    print("## final")
    # Try to get final with wrong node ID
    final_status, final_text = get_query_final(qid, "XXX")
    final_text = final_text.replace(qid, "QID").replace(node_id, "NODE")
    final_text = re.sub(r"at.*secs", "...", final_text)
    print(final_text)

    print("")
    print("# ok")
    print("## page")
    # Get page with correct node ID
    page_status, page_text = get_query_page(qid, 0, node_id)
    page_data = json.loads(page_text).get("data", [])
    print(json.dumps(page_data))

    print("## kill")
    # Kill with correct node ID
    kill_status, kill_text = kill_query(qid, node_id)
    print(kill_status)

    print("## final")
    # Get final with correct node ID
    final_status, final_text = get_query_final(qid, node_id)
    final_data = json.loads(final_text)
    print(json.dumps(final_data.get("next_uri")))
