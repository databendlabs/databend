import requests
import json


# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
auth = ("root", "")


def execute_query(payload_data):
    """Execute query with custom payload data"""
    response = requests.post(
        query_url,
        auth=auth,
        headers={"Content-Type": "application/json"},
        data=payload_data,
    )
    return response.json()


def test_json_response_errors():
    # Test 1: Invalid SQL query - select non-existent column
    result1 = execute_query('{"sql": "select a", "pagination": { "wait_time_secs": 5}}')
    assert result1.get("state", "Unknown") == "Failed"
    assert result1["error"] == json.loads(
        """{"code": 1065, "message": "error: \\n  --> SQL:1:8\\n  |\\n1 | select a\\n  |        ^ column a doesn't exist\\n\\n"}"""
    )

    # Test 2: Malformed JSON - missing quote before sql key
    result2 = execute_query(
        '{sql": "select * from tx", "pagination": { "wait_time_secs": 2}}'
    )
    assert result2 == json.loads(
        '{"error": {"code": 400, "message": "parse error: key must be a string at line 1 column 2"}}'
    )

    # Test 3: Invalid endpoint URL
    response3 = requests.post(
        "http://localhost:8000/v1/querq/",  # Note: wrong endpoint
        auth=auth,
        headers={"Content-Type": "application/json"},
        data='{"sql": "select * from tx", "pagination": { "wait_time_secs": 2}}',
    )
    assert response3.json() == json.loads(
        '{"error": {"code": 404, "message": "not found"}}'
    )
