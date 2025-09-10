import requests
import random
from suites.utils import comparison_output

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
auth = ("root", "")


def execute_query(sql, query_id):
    """Execute SQL query via HTTP API with specific query ID"""
    headers = {"Content-Type": "application/json", "x-databend-query-id": query_id}
    payload = {"sql": sql, "pagination": {"wait_time_secs": 6}}
    response = requests.post(query_url, auth=auth, headers=headers, json=payload)
    return response.json()


def kill_query(query_id):
    """Kill a running query"""
    kill_url = f"http://localhost:8000/v1/query/{query_id}/kill"
    response = requests.get(kill_url, auth=auth)
    return response.status_code, response.text


def get_query_page(query_id, page_number):
    """Get a specific page of query results"""
    page_url = f"http://localhost:8000/v1/query/{query_id}/page/{page_number}"
    response = requests.get(page_url, auth=auth)
    return response.status_code, response.text


def get_query_final(query_id):
    """Get final result of a query"""
    final_url = f"http://localhost:8000/v1/query/{query_id}/final"
    response = requests.get(final_url, auth=auth)
    return response.json()


@comparison_output(
    """## query
"Running"
## kill
200
## page
{"error":{"code":400,"message":"[HTTP-QUERY] Query ID QID canceled"}}
400
## final
{'code': 1043, 'message': 'canceled by client'}
"""
)
def test_kill_query():
    # Generate random query ID
    qid = f"my_query_for_kill_{random.randint(1000, 9999)}"

    print("## query")
    # Start a long-running query
    query_result = execute_query(
        "select sleep(0.5), number from numbers(15000000000);", qid
    )
    print(f'"{query_result.get("state", "Unknown")}"')

    print("## kill")
    # Kill the query
    kill_status, kill_text = kill_query(qid)
    print(kill_status)

    print("## page")
    # Try to get page 0 - should fail with cancellation error
    page_status, page_text = get_query_page(qid, 0)
    # Replace actual query ID with "QID" for consistent output
    page_text = page_text.replace(qid, "QID")
    print(page_text)
    print(page_status)

    print("## final")
    # Get final result - should show cancellation error
    final_result = get_query_final(qid)
    print(final_result["error"])
