import json
import uuid

import requests
import pyarrow.ipc as ipc

auth = ("root", "")


def do_query(query, pagination, query_id):
    url = f"http://localhost:8000/v1/query"
    payload = {
        "sql": query,
    }
    if pagination:
        payload["pagination"] = pagination
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-databend-query-id": query_id,
    }

    return requests.post(url, headers=headers, json=payload, auth=auth)


def test_result_format_settings():
    sql = 'select 1'
    query_id = str(uuid.uuid4())
    do_query('select 1', None, query_id)

    resp = do_query('select 2', None, query_id)
    print(resp.json())
    assert resp.json()['data'][0][0] == "1"