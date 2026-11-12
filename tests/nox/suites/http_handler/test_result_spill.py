import time

import pytest
import requests

auth = ("root", "")


def do_query(query, session, pagination):
    url = "http://localhost:8000/v1/query"
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

    response = requests.post(url, headers=headers, json=payload, auth=auth, timeout=10)
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
    assert resp["error"] is None

    # print(resp["state"])
    # print(resp["stats"])

    rows = len(resp["data"])
    for _ in range(30):
        if resp.get("next_uri") is None:
            break

        uri = f"http://localhost:8000/{resp['next_uri']}"
        resp = requests.get(uri, auth=auth).json()
        cur_rows = len(resp["data"])
        assert resp["error"] is None, resp
        if rows < 96:
            assert cur_rows == 4
        rows += cur_rows

        # print(resp["state"])
        # print(resp["stats"])

        # get same page again
        assert requests.get(uri, auth=auth).json()["error"] is None, resp

    assert rows == 97


@pytest.mark.parametrize("enable_spill", ["0", "1"])
def test_result_spill_ordered_pagination(enable_spill):
    session = {
        "settings": {
            "enable_result_set_spilling": enable_spill,
            "max_block_size": "10",
            "max_threads": "2",
        }
    }
    pagination = {
        "wait_time_secs": 0,
        "max_rows_in_buffer": 4,
        "max_rows_per_page": 4,
    }
    resp = do_query(
        "select number from numbers(97) order by number",
        session=session,
        pagination=pagination,
    )
    values = []
    deadline = time.monotonic() + 30
    while True:
        assert resp["error"] is None, resp
        values.extend(int(row[0]) for row in resp["data"])
        assert values == list(range(len(values)))
        if resp.get("next_uri") is None:
            break
        assert time.monotonic() < deadline, resp
        if not resp["data"]:
            time.sleep(0.01)
        uri = f"http://localhost:8000/{resp['next_uri'].lstrip('/')}"
        resp = requests.get(uri, auth=auth, timeout=10).json()
        # Retrying a non-empty page must return the same ordered data.
        if resp["data"]:
            retry = requests.get(uri, auth=auth, timeout=10).json()
            assert retry["error"] is None, retry
            assert retry["data"] == resp["data"]

    assert values == list(range(97))


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
    assert resp["error"] is None

    time.sleep(1)

    rows = len(resp["data"])
    for _ in range(30):
        if resp.get("next_uri") is None:
            break

        uri = f"http://localhost:8000/{resp['next_uri']}"
        resp = requests.get(uri, auth=auth).json()
        cur_rows = len(resp["data"])
        assert resp["error"] is None, resp
        if rows < 96:
            assert cur_rows == 4
        rows += cur_rows

        assert resp["state"] == "Succeeded"

        # get same page again
        assert requests.get(uri, auth=auth).json()["error"] is None, resp

    assert rows == 97
