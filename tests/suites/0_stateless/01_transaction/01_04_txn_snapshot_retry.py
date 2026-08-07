#!/usr/bin/env python3

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import Random

import mysql.connector
from mysql.connector import errors as mysql_errors


TABLE_NAME = "txn_snapshot_retry_concurrency"
STATS_DATABASE = "txn_snapshot_retry_stats_db"
STATS_TABLE = "txn_snapshot_retry_stats"
NUM_THREADS = 8
TRANSACTIONS_PER_THREAD = 3
ROWS_PER_TRANSACTION = 2
MAX_RETRIES = 8
RETRY_SLEEP_RANGE = (0.01, 0.05)
VALUE_GAP = 1_000_000

HOST = os.getenv("QUERY_MYSQL_HANDLER_HOST", "127.0.0.1")
PORT = int(os.getenv("QUERY_MYSQL_HANDLER_PORT", "3307"))
USER = os.getenv("QUERY_MYSQL_HANDLER_USER", "root")
PASSWORD = os.getenv("QUERY_MYSQL_HANDLER_PASSWORD", "root")


def create_connection(autocommit=False):
    conn = mysql.connector.connect(
        host=HOST, port=PORT, user=USER, passwd=PASSWORD, autocommit=autocommit
    )
    cursor = conn.cursor()
    return conn, cursor


def drain(cursor):
    try:
        cursor.fetchall()
    except mysql_errors.InterfaceError:
        pass


def execute(cursor, sql: str) -> None:
    cursor.execute(sql)
    drain(cursor)


def fetch_one(cursor, sql: str):
    cursor.execute(sql)
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise AssertionError(f"Expected one row for {sql}, got {rows}")
    return rows[0]


def fetch_stats(cursor):
    return fetch_one(
        cursor,
        f"""
        SELECT stats_row_count, actual_row_count, distinct_count
        FROM system.statistics
        WHERE database = '{STATS_DATABASE}'
          AND table = '{STATS_TABLE}'
          AND column_name = 'a'
        """,
    )


def assert_stats(cursor, expected_rows: int) -> None:
    stats_row_count, actual_row_count, distinct_count = fetch_stats(cursor)
    if stats_row_count != expected_rows or actual_row_count != expected_rows:
        raise AssertionError(
            "Expected stats_row_count and actual_row_count "
            f"to be {expected_rows}; got stats_row_count={stats_row_count}, "
            f"actual_row_count={actual_row_count}, distinct_count={distinct_count}"
        )


def use_stats_database(cursor) -> None:
    execute(cursor, f"USE {STATS_DATABASE}")
    execute(cursor, "SET enable_table_snapshot_stats = 1")


def run_transaction_batch(thread_id: int) -> None:
    conn, cursor = create_connection()
    rng = Random(thread_id)

    try:
        for tx_index in range(TRANSACTIONS_PER_THREAD):
            base_value = thread_id * VALUE_GAP + tx_index * ROWS_PER_TRANSACTION
            values_clause = ", ".join(
                f"({base_value + offset})" for offset in range(ROWS_PER_TRANSACTION)
            )

            attempts = 0
            while True:
                attempts += 1
                try:
                    cursor.execute("BEGIN")
                    drain(cursor)

                    cursor.execute(f"INSERT INTO {TABLE_NAME} VALUES {values_clause}")
                    drain(cursor)

                    cursor.execute("COMMIT")
                    drain(cursor)
                    break
                except Exception:
                    try:
                        cursor.execute("ROLLBACK")
                        drain(cursor)
                    except Exception:
                        pass

                    cursor.close()
                    conn.close()

                    if attempts >= MAX_RETRIES:
                        raise

                    time.sleep(rng.uniform(*RETRY_SLEEP_RANGE))
                    conn, cursor = create_connection()
    finally:
        cursor.close()
        conn.close()


def verify_concurrent_transaction_retry() -> None:
    setup_conn, setup_cursor = create_connection()
    try:
        execute(setup_cursor, f"DROP TABLE IF EXISTS {TABLE_NAME}")
        execute(setup_cursor, f"CREATE TABLE {TABLE_NAME} (id BIGINT)")
    finally:
        setup_cursor.close()
        setup_conn.close()

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [
            executor.submit(run_transaction_batch, thread_id)
            for thread_id in range(NUM_THREADS)
        ]
        for future in as_completed(futures):
            future.result()

    verify_conn, verify_cursor = create_connection()
    try:
        expected_rows = NUM_THREADS * TRANSACTIONS_PER_THREAD * ROWS_PER_TRANSACTION

        verify_cursor.execute(
            f"SELECT COUNT(*) AS cnt, COUNT(DISTINCT id) AS uniq FROM {TABLE_NAME}"
        )
        counts = verify_cursor.fetchall()[0]
        total_count, distinct_count = counts[0], counts[1]

        if total_count != expected_rows or distinct_count != expected_rows:
            raise AssertionError(
                f"Expected {expected_rows} rows, got total={total_count}, distinct={distinct_count}"
            )

        verify_cursor.execute(
            f"SELECT id FROM {TABLE_NAME} GROUP BY id HAVING COUNT(*) > 1 LIMIT 1"
        )
        duplicates = verify_cursor.fetchall()
        if duplicates:
            raise AssertionError(f"found duplicated segments: {duplicates}")

        execute(verify_cursor, f"DROP TABLE IF EXISTS {TABLE_NAME}")
    finally:
        verify_cursor.close()
        verify_conn.close()


def verify_snapshot_stats_after_retry() -> None:
    setup_conn, setup_cursor = create_connection(autocommit=True)
    conn_a = cursor_a = conn_b = cursor_b = None
    try:
        execute(setup_cursor, f"DROP DATABASE IF EXISTS {STATS_DATABASE}")
        execute(setup_cursor, f"CREATE DATABASE {STATS_DATABASE}")
        use_stats_database(setup_cursor)
        execute(setup_cursor, f"CREATE TABLE {STATS_TABLE}(a INT)")
        execute(
            setup_cursor,
            f"INSERT INTO {STATS_TABLE} SELECT number::int FROM numbers(100)",
        )
        execute(setup_cursor, f"ANALYZE TABLE {STATS_TABLE}")
        assert_stats(setup_cursor, 100)

        conn_a, cursor_a = create_connection(autocommit=True)
        conn_b, cursor_b = create_connection(autocommit=True)
        use_stats_database(cursor_a)
        use_stats_database(cursor_b)

        execute(cursor_a, "BEGIN")
        execute(
            cursor_a,
            f"INSERT INTO {STATS_TABLE} "
            "SELECT (1000 + number)::int FROM numbers(10)",
        )
        count_in_a = fetch_one(
            cursor_a,
            f"SELECT count(*) FROM {STATS_TABLE}",
        )[0]
        if count_in_a != 110:
            raise AssertionError(f"Expected session A to see 110 rows, got {count_in_a}")

        execute(
            cursor_b,
            f"INSERT INTO {STATS_TABLE} "
            "SELECT (2000 + number)::int FROM numbers(5)",
        )
        count_in_b = fetch_one(
            cursor_b,
            f"SELECT count(*) FROM {STATS_TABLE}",
        )[0]
        if count_in_b != 105:
            raise AssertionError(f"Expected session B to see 105 rows, got {count_in_b}")
        assert_stats(cursor_b, 105)

        execute(cursor_a, "COMMIT")
        count_after_commit = fetch_one(
            cursor_a,
            f"SELECT count(*) FROM {STATS_TABLE}",
        )[0]
        if count_after_commit != 115:
            raise AssertionError(
                f"Expected 115 rows after session A commit, got {count_after_commit}"
            )
        assert_stats(cursor_a, 115)
    finally:
        for cursor in (cursor_a, cursor_b):
            if cursor is not None:
                cursor.close()
        for conn in (conn_a, conn_b):
            if conn is not None:
                conn.close()
        execute(setup_cursor, f"DROP DATABASE IF EXISTS {STATS_DATABASE}")
        setup_cursor.close()
        setup_conn.close()


def main() -> None:
    verify_concurrent_transaction_retry()
    verify_snapshot_stats_after_retry()
    print("Transaction snapshot retry looks good")


if __name__ == "__main__":
    main()
