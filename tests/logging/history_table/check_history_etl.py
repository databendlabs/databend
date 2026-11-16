#!/usr/bin/env python3
"""Run via `bash tests/logging/test-history-tables.sh` in both storage modes.

Requires built query/meta binaries, a test license and MinIO on :9900 (testbucket).
The runner resets .databend and starts workers plus a separate-cluster observer.
A corrupt Parquet file triggers four failures; removal must restore ingestion
without restarting, while retaining the last error. Status is read from the
observer with ETL disabled to verify tenant-wide visibility through meta.
"""

import json
import time
import urllib.request
import uuid

WRITER = "http://localhost:8000"
OBSERVER = "http://localhost:8004"
STAGE = "log_1f93b76af0bd4b1d8e018667865fbc65"


def request(
    base, path, data=None, content_type="application/json", method=None, headers=None
):
    req = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={
            "Authorization": "Basic cm9vdDo=",
            "Content-Type": content_type,
            **(headers or {}),
        },
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)


def query(base, sql):
    result = request(
        base,
        "/v1/query",
        json.dumps({"sql": sql, "pagination": {"wait_time_secs": 10}}).encode(),
    )
    query_id = result["id"]
    rows = []
    deadline = time.monotonic() + 60
    while True:
        if result.get("error"):
            raise AssertionError(f"{sql}: {result['error']}")
        rows.extend(result.get("data", []))
        if not result.get("next_uri") or result["next_uri"] == result.get("final_uri"):
            break
        assert time.monotonic() < deadline, f"Query timed out: {sql}"
        result = request(base, result["next_uri"])
    if result.get("final_uri"):
        request(base, result["final_uri"])
    assert result["state"] == "Succeeded", result
    return rows, query_id


def status():
    rows, _ = query(
        OBSERVER,
        "SELECT batch_number, last_success_time, last_error_time, last_error "
        "FROM system.history_etl WHERE table_name = 'log_history'",
    )
    assert len(rows) == 1, rows
    batch, success, error_time, error = rows[0]
    return int(batch), success, error_time, error


def wait_for(description, check, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = check()
        if last:
            return last
        time.sleep(1)
    raise AssertionError(f"Timed out waiting for {description}; ETL status: {status()}")


def main():
    # The observer has no history ETL enabled and belongs to a different cluster/warehouse.
    def remote_heartbeat():
        rows, _ = query(
            OBSERVER,
            "SELECT count(*) FROM system.history_etl WHERE table_name = 'log_history' "
            "AND heartbeat_node_id NOT IN (SELECT name FROM system.clusters)",
        )
        return int(rows[0][0]) == 1

    wait_for("a heartbeat owned outside the observer's cluster", remote_heartbeat)
    before = status()
    assert before[1] is not None, before

    # COPY reads every file in this stage as Parquet. A bad footer causes a real,
    # repeatable ETL failure without modifying the history table or meta records.
    filename = f"history_etl_failure_{uuid.uuid4().hex}.parquet"
    boundary = uuid.uuid4().hex
    data = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="upload"; filename="{filename}"\r\n'
        "Content-Type: application/octet-stream\r\n\r\n"
        "This file deliberately has an invalid Parquet footer.\r\n"
        f"--{boundary}--\r\n"
    ).encode()
    uploaded = False
    try:
        # Arm cleanup before sending: a failed HTTP response may still leave an object behind.
        uploaded = True
        response = request(
            WRITER,
            "/v1/upload_to_stage",
            data,
            f"multipart/form-data; boundary={boundary}",
            "PUT",
            {"x-databend-stage-name": STAGE},
        )
        assert response["state"] == "SUCCESS", response

        previous_time = before[2]
        failed = None
        # Four persistent failures used to terminate the worker. Require recovery after
        # reaching that boundary, and observe each overwrite instead of sleeping blindly.
        for attempt in range(4):

            def next_error():
                current = status()
                return (
                    current
                    if current[2] is not None and current[2] != previous_time
                    else None
                )

            current = wait_for(f"ETL error report {attempt + 1}", next_error)
            assert current[3] and len(current[3].encode()) <= 1024, current
            if failed is not None:
                assert current[:2] == failed[:2], (failed, current)
            failed = current
            previous_time = current[2]
            print(f"Observed ETL error report {attempt + 1}: {current[2]}", flush=True)

        query(WRITER, f"REMOVE @{STAGE}/{filename}")
        uploaded = False
        _, marker_id = query(WRITER, "SELECT 'history ETL recovery marker'")

        def recovered():
            current = status()
            return (
                current if current[0] > failed[0] and current[1] > failed[1] else None
            )

        recovered_status = wait_for(
            "checkpoint progress without restarting query", recovered
        )
        assert recovered_status[2:] == failed[2:], (failed, recovered_status)

        def marker_ingested():
            rows, _ = query(
                WRITER,
                f"SELECT count(*) FROM system_history.query_history WHERE query_id = '{marker_id}'",
            )
            return int(rows[0][0]) == 1

        wait_for("query history to ingest the recovery marker", marker_ingested)
        assert status()[2:] == failed[2:]
        print(
            "History ETL failure, overwrite, recovery and cross-cluster visibility passed."
        )
    finally:
        if uploaded:
            query(WRITER, f"REMOVE @{STAGE}/{filename}")


if __name__ == "__main__":
    main()
