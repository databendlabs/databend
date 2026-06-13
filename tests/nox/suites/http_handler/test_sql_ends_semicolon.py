import requests
import json
from suites.utils import comparison_output

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
auth = ("root", "")


def execute_query(sql):
    """Execute SQL query via HTTP API"""
    payload = {"sql": sql, "pagination": {"wait_time_secs": 5}}
    response = requests.post(
        query_url, auth=auth, headers={"Content-Type": "application/json"}, json=payload
    )
    return response.json()


@comparison_output(
    """>>>> create or replace table t_09_0002 (a int)
<<<<
"Succeeded"
null
"Succeeded"
null
"Succeeded"
null
"Succeeded"
null"""
)
def test_sql_ends_with_semicolon():
    # Create table
    assert execute_query("create or replace table t_09_0002 (a int)")["error"] == None
    print(">>>> create or replace table t_09_0002 (a int)")
    print("<<<<")

    # Test INSERT statements with semicolons
    queries = [
        "insert into t_09_0002 from @~/not_exist file_format=(type=csv);",
        "insert into t_09_0002 select 1;",
        "insert into t_09_0002 values (1);",
        "select 1;",
    ]

    for query in queries:
        result = execute_query(query)
        print(f'"{result.get("state", "Unknown")}"')
        print(json.dumps(result["error"]))
