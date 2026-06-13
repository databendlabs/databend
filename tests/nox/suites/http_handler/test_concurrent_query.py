import requests


# Define the URLs and credentials
query_url = "http://127.0.0.1:8000/v1/query"
auth = ("root", "")
session_id = "065142ee-68e8-470b-a4d6-2ed67f90e2ae"


def execute_query_with_cookies(query_id, payload, session_data=None):
    """Execute SQL query via HTTP API with cookies and specific headers"""
    headers = {
        "x-databend-query-id": query_id,
        "Content-Type": "application/json",
    }

    if session_data:
        payload["session"] = session_data

    cookies = {"cookie_enabled": "true", "session_id": session_id}

    response = requests.post(
        query_url, headers=headers, json=payload, auth=auth, cookies=cookies
    )
    return response.json()


def get_query_page(query_id, page_number):
    """Get specific page of query results"""
    cookies = {"cookie_enabled": "true", "session_id": session_id}

    url = f"http://127.0.0.1:8000/v1/query/{query_id}/page/{page_number}"
    response = requests.get(url, auth=auth, cookies=cookies)
    return response.json()


def test_concurrent_queries_with_session():
    """Test concurrent queries with session sharing and cookie handling"""
    # First query with pagination

    resp1 = execute_query_with_cookies(
        "qq1",
        {"sql": "select * from numbers(10);", "pagination": {"max_rows_per_page": 2}},
    )
    assert len(resp1.get("data", [])) == 2
    assert resp1.get("error") == None
    assert not resp1.get("session", {}).get("need_sticky", False)

    # Second query referencing first query's session
    session_data = {
        "internal": '{"variables": [], "last_query_result_cache_key":"x"}',
        "last_query_ids": ["qq1"],
    }
    resp2 = execute_query_with_cookies(
        "qq2", {"sql": "select * from numbers(10);"}, session_data
    )
    assert len(resp2.get("data", [])) == 10
    assert resp2.get("error") == None
    assert not resp2.get("session", {}).get("need_sticky", False)

    # Get next page of first query - session field should be empty
    page_resp = get_query_page("qq1", 1)
    assert len(page_resp.get("data", [])) == 2
    assert page_resp.get("error") == None
    assert page_resp.get("session") == None
