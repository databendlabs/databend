import requests
from suites.utils import comparison_output

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
auth = ("root", "")


def execute_query(sql, query_id=None):
    """Execute SQL query via HTTP API with optional query ID"""
    headers = {"Content-Type": "application/json"}
    if query_id:
        headers["X-DATABEND-QUERY-ID"] = query_id

    payload = {"sql": sql, "pagination": {"wait_time_secs": 5}}
    response = requests.post(query_url, auth=auth, headers=headers, json=payload)
    return response.json()


def get_query_page(query_id, page):
    """Get a specific page of query results"""
    page_url = f"http://localhost:8000/v1/query/{query_id}/{page}"
    response = requests.get(
        page_url, auth=auth, headers={"Content-Type": "application/json"}
    )
    return response.json()


def test_large_row():
    qid = "09_004"

    # Execute query with large JSON aggregation
    result = execute_query(
        "select json_array_agg(json_object('num',number)), (number % 2) as s from numbers(2000000) group by s;",
        qid,
    )
    assert len(result.get("data", [])) == 1

    # Get page 1 and page 2 of the results
    page1_result = get_query_page(qid, "page/1")
    assert len(page1_result.get("data", [])) == 1

    page2_result = get_query_page(qid, "page/2")
    assert len(page2_result.get("data", [])) == 0

    result = execute_query("SELECT repeat(number::string, 2000) from numbers(100000)")
    rows = len(result["data"])

    qid = result["id"]

    for i in range(1, 1000):
        result = get_query_page(qid, f"page/{i}")
        rows += len(result["data"])
        if result["next_uri"] == result["final_uri"]:
            assert get_query_page(qid, "final")["error"] == None
            assert rows == 100000
            return
