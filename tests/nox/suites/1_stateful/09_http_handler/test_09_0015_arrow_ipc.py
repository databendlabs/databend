import json

import requests
import pyarrow.ipc as ipc

auth = ("root", "")


def do_query(query, session, pagination):
    url = f"http://localhost:8000/v1/query"
    payload = {
        "sql": query,
    }
    if session:
        payload["session"] = session
    if pagination:
        payload["pagination"] = pagination
    headers = {
        "Accept": "application/vnd.apache.arrow.stream",
        "Content-Type": "application/json",
    }

    return requests.post(url, headers=headers, json=payload, auth=auth)


def test_arrow_ipc():
    pagination = {
        "max_rows_per_page": 20,
    }
    resp = do_query("select * from numbers(97)", session=None, pagination=pagination)

    # print("content", len(resp.content))
    # IpcWriteOptions(alignment 64 compression None) content: 1672
    # IpcWriteOptions(alignment 8 compression lz4) content: 1448

    def read_response(resp):
        if resp.headers["Content-Type"] != "application/vnd.apache.arrow.stream":
            header = resp.json()
            assert len(header["data"]) == 0
            assert header["error"] == None
            return (header, None)
        else:
            reader = ipc.open_stream(resp.content)
            header = json.loads(reader.schema.metadata[b"response_header"])
            assert header["error"] == None
            return (header, reader)

    rows = []

    def drain_reader(reader):
        if reader:
            with reader:
                for batch in reader:
                    rows.extend([x.as_py() for x in batch["number"]])

    (header, reader) = read_response(resp)
    drain_reader(reader)

    for _ in range(30):
        if header.get("next_uri") == None:
            break

        uri = f"http://localhost:8000/{header['next_uri']}"
        resp = requests.get(
            uri, auth=auth, headers={"Accept": "application/vnd.apache.arrow.stream"}
        )
        (header, reader) = read_response(resp)
        drain_reader(reader)

    rows.sort()
    assert rows == [x for x in range(97)]


def query_page_0(sql):
    query_url = "http://localhost:8000/v1/query"

    data = {"sql": sql, "pagination": {"wait_time_secs": 5}}
    data = json.dumps(data)
    response = requests.post(
        query_url,
        auth=auth,
        headers={"Content-Type": "application/json"},
        data=data,
    )
    return response.json()


def test_result_format_settings():
    timezone = "Asia/Shanghai"
    r = query_page_0(f"settings (timezone='{timezone}') select 1")
    assert r.get("settings", {}).get("timezone") == timezone

    geometry_output_format = "EWKB"
    r = query_page_0(
        f"settings (geometry_output_format='{geometry_output_format}') select 1"
    )
    assert r.get("settings", {}).get("geometry_output_format") == geometry_output_format
