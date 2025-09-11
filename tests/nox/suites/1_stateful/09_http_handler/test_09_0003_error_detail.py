import requests
import os


# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
upload_url = "http://localhost:8000/v1/upload_to_stage"
auth = ("root", "")


def execute_sql(sql):
    """Execute SQL query via HTTP API"""
    payload = {"sql": sql, "pagination": {"wait_time_secs": 6}}
    response = requests.post(
        query_url, auth=auth, headers={"Content-Type": "application/json"}, json=payload
    )
    return response.json()


def upload_to_stage(stage_name, file_path):
    """Upload file to stage"""
    with open(file_path, "rb") as f:
        files = {"upload": f}
        response = requests.put(
            upload_url,
            auth=auth,
            headers={"x-databend-stage-name": stage_name},
            files=files,
        )
    return response.json()


def test_error_detail():
    # Setup: drop existing table and stage
    assert execute_sql("drop table if exists products")["error"] == None
    assert execute_sql("drop stage if exists s1")["error"] == None

    # Create stage and table
    assert execute_sql("CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV)")["error"] == None
    assert execute_sql("remove @s1")["error"] == None
    assert (
        execute_sql("create table products (id int, name string, description string)")[
            "error"
        ]
        == None
    )

    # Upload CSV file to stage (select.csv has 2 columns, table has 3 columns)
    csv_file_path = os.path.join(
        os.path.dirname(__file__), "../../../../data/csv/select.csv"
    )
    upload_result = upload_to_stage("s1", csv_file_path)
    assert not "data" in upload_result

    # Try to copy from stage to table - should fail with column mismatch error
    copy_result = execute_sql(
        "copy /*+ set_var(enable_distributed_copy_into = 0) */ into products (id, name, description) from @s1/"
    )

    assert copy_result["error"] == {
        "code": 1046,
        "message": "Number of columns in file (2) does not match that of the corresponding table (3)",
        "detail": "at file 'select.csv', line 1",
    }

    # Cleanup
    assert execute_sql("drop table if exists products")["error"] == None
    assert execute_sql("drop stage if exists s1")["error"] == None
