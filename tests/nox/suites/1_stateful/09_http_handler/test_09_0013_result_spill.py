import requests
import time

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
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, json=payload, auth=auth)
    return response.json()


def test_disable_result_spill():
    session = {
        "settings": {
            "enable_result_set_spilling": "0",
            "max_block_size": "10",
            "max_threads": "2",
        }
    }
    pagination = {
        "wait_time_secs": 2,
        "max_rows_in_buffer": 4,
        "max_rows_per_page": 4,
    }
    resp = do_query("select * from numbers(97)", session=session, pagination=pagination)
    assert resp["error"] == None

    # print(resp["state"])
    # print(resp["stats"])

    rows = len(resp["data"])
    for _ in range(30):
        if resp.get("next_uri") == None:
            break

        uri = f"http://localhost:8000/{resp['next_uri']}"
        resp = requests.get(uri, auth=auth).json()
        cur_rows = len(resp["data"])
        assert resp["error"] == None, resp
        if rows < 96:
            assert cur_rows == 4
        rows += cur_rows

        # print(resp["state"])
        # print(resp["stats"])

        # get same page again
        assert requests.get(uri, auth=auth).json()["error"] == None, resp

    assert rows == 97


def test_enable_result_spill():
    session = {
        "settings": {
            "enable_result_set_spilling": "1",
            "max_block_size": "10",
            "max_threads": "2",
        }
    }
    pagination = {
        "wait_time_secs": 2,
        "max_rows_in_buffer": 4,
        "max_rows_per_page": 4,
    }
    resp = do_query("select * from numbers(97)", session=session, pagination=pagination)
    assert resp["error"] == None

    time.sleep(1)

    rows = len(resp["data"])
    for _ in range(30):
        if resp.get("next_uri") == None:
            break

        uri = f"http://localhost:8000/{resp['next_uri']}"
        resp = requests.get(uri, auth=auth).json()
        cur_rows = len(resp["data"])
        assert resp["error"] == None, resp
        if rows < 96:
            assert cur_rows == 4
        rows += cur_rows

        assert resp["state"] == "Succeeded"

        # get same page again
        assert requests.get(uri, auth=auth).json()["error"] == None, resp

    assert rows == 97
