import requests

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
auth = ("root", "")


def execute_sql(sql):
    """Execute SQL query via HTTP API"""
    payload = {"sql": sql, "pagination": {"wait_time_secs": 3}}
    response = requests.post(
        query_url, auth=auth, headers={"Content-Type": "application/json"}, json=payload
    )
    return response.json()


def test_invalid_utf8():
    # Setup: create table and insert invalid UTF-8 data
    assert execute_sql("drop table if exists t1")["error"] == None
    assert execute_sql("create table t1(a varchar)")["error"] == None
    assert execute_sql("insert INTO t1 VALUES(FROM_BASE64('0Aw='))")["error"] == {
        "code": 1006,
        "message": "invalid utf8 sequence while evaluating function `to_string(D00C)` in expr `CAST(from_hex('D00C')::string AS String NULL)`",
    }

    # Query the table
    assert execute_sql("select * from t1")["data"] == []
