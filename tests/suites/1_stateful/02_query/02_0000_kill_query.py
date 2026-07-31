#!/usr/bin/env python3

import os
import sys
import time

import mysql.connector

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

from native_client import NativeClient, prompt  # noqa: E402

QUERY_TIMEOUT_SECONDS = 10
POLL_INTERVAL_SECONDS = 0.05


def get_query_connection_id(cursor, marker):
    cursor.execute(
        "SELECT mysql_connection_id FROM system.processes "
        f"WHERE extra_info LIKE '%{marker}%' "
        "AND extra_info NOT LIKE '%system.processes%'"
    )
    result = cursor.fetchone()
    return None if result is None else result[0]


def wait_for_query_to_start(cursor, marker):
    deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        connection_id = get_query_connection_id(cursor, marker)
        if connection_id is not None:
            return connection_id
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"query matching {marker!r} did not start")


def wait_for_query_to_stop(cursor, marker):
    deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if get_query_connection_id(cursor, marker) is None:
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"killed query matching {marker!r} did not stop")


def run_and_kill_query(client, cursor, query, marker):
    client.send(query)
    connection_id = wait_for_query_to_start(cursor, marker)
    cursor.execute(f"KILL QUERY {connection_id}")
    wait_for_query_to_stop(cursor, marker)
    client.expect(prompt)


def main():
    # One MySQL client runs each long query while a second connection kills it.
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="root", passwd="root", port="3307"
    )
    mycursor = mydb.cursor()

    try:
        with NativeClient(name="client1>") as client1:
            client1.expect(prompt)

            aggregate_query = (
                "SELECT max(number), sum(number) FROM numbers_mt(100000000000) "
                "GROUP BY number % 3, number % 4, number % 5 LIMIT 10;"
            )
            run_and_kill_query(
                client1,
                mycursor,
                aggregate_query,
                "SELECT max(number)",
            )

            rows = 30000
            marker = "outer_left_hash_join_interrupt_regression"
            outer_left_join_query = f"""
                SELECT count(*)
                FROM numbers({rows}) AS t1
                LEFT JOIN (
                    SELECT number + {rows + 1} AS number
                    FROM numbers({rows})
                ) AS {marker}
                ON  (t1.number = {marker}.number AND t1.number % 2 = 0)
                 OR (t1.number + 1 = {marker}.number AND t1.number % 2 = 1)
                WHERE {marker}.number IS NULL
            """

            outer_left_join_statement = (
                "SETTINGS (max_threads=8, max_block_size=65536) "
                f"{outer_left_join_query};"
            )
            run_and_kill_query(
                client1,
                mycursor,
                outer_left_join_statement,
                marker,
            )

            spilling_aggregate_query = (
                "SETTINGS (force_aggregate_data_spill = 1) "
                "SELECT max(number), sum(number) FROM numbers_mt(100000000000) "
                "GROUP BY number LIMIT 10;"
            )
            run_and_kill_query(
                client1,
                mycursor,
                spilling_aggregate_query,
                "force_aggregate_data_spill",
            )
    finally:
        mycursor.close()
        mydb.close()


if __name__ == "__main__":
    main()
