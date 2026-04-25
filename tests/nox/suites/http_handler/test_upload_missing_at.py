import requests


query_url = "http://localhost:8000/v1/query"
streaming_url = "http://localhost:8000/v1/streaming_load"
upload_url = "http://localhost:8000/v1/upload_to_stage"
auth = ("root", "")


def execute_sql(sql):
    payload = {"sql": sql, "pagination": {"wait_time_secs": 6}}
    response = requests.post(
        query_url, auth=auth, headers={"Content-Type": "application/json"}, json=payload
    )
    return response.json()


def missing_at_upload():
    return {"upload": (None, "./abc.json")}


def test_upload_to_stage_requires_file_upload():
    stage_name = "missing_at_stage"
    assert execute_sql(f"drop stage if exists {stage_name}")["error"] == None
    assert execute_sql(f"create stage {stage_name}")["error"] == None

    try:
        response = requests.put(
            upload_url,
            auth=auth,
            headers={"x-databend-stage-name": stage_name},
            files=missing_at_upload(),
        )

        assert response.status_code == 400
        assert "expected a file upload with a filename" in response.text
        assert "did you forget the '@'" in response.text.lower()
        assert "upload=@/path/to/file" in response.text
    finally:
        assert execute_sql(f"drop stage if exists {stage_name}")["error"] == None


def test_streaming_load_requires_file_upload():
    table_name = "missing_at_streaming"
    assert execute_sql(f"drop table if exists {table_name}")["error"] == None
    assert execute_sql(f"create table {table_name} (a string)")["error"] == None

    try:
        response = requests.put(
            streaming_url,
            auth=auth,
            headers={
                "X-Databend-SQL": f"insert into {table_name} from @_databend_load file_format = (type = csv)",
                "x-databend-query-id": "missing-at-streaming-load",
            },
            files=missing_at_upload(),
        )

        assert response.status_code == 400
        assert "expected a file upload with a filename" in response.text
        assert "did you forget the '@'" in response.text.lower()
        assert "upload=@/path/to/file" in response.text
    finally:
        assert execute_sql(f"drop table if exists {table_name}")["error"] == None
