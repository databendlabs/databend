#!/usr/bin/env python3

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import Random

import mysql.connector
from mysql.connector import errors as mysql_errors


DB_NAME = "db08_table_ref_branch_txn_snapshot_retry"
TABLE_NAME = "t_branch_txn_snapshot_retry"
BRANCH_NAME = "b1"

NUM_THREADS = 8
TRANSACTIONS_PER_THREAD = 4
ROWS_PER_TRANSACTION = 2
MAX_RETRIES = 8
RETRY_SLEEP_RANGE = (0.01, 0.05)
VALUE_GAP = 1_000_000

HOST = os.getenv("QUERY_MYSQL_HANDLER_HOST", "127.0.0.1")
PORT = int(os.getenv("QUERY_MYSQL_HANDLER_PORT", "3307"))
USER = os.getenv("QUERY_MYSQL_HANDLER_USER", "root")
PASSWORD = os.getenv("QUERY_MYSQL_HANDLER_PASSWORD", "root")


def create_connection(database: str | None):
    conn = mysql.connector.connect(
        host=HOST,
        port=PORT,
        user=USER,
        passwd=PASSWORD,
        database=database,
        autocommit=False,
    )
    cursor = conn.cursor()
    return conn, cursor


def drain(cursor):
    try:
        cursor.fetchall()
    except mysql_errors.InterfaceError:
        pass


def run_transaction_batch(thread_id: int) -> None:
    conn, cursor = create_connection(DB_NAME)
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

                    cursor.execute(
                        f"INSERT INTO {TABLE_NAME}/{BRANCH_NAME} VALUES {values_clause}"
                    )
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
                    conn, cursor = create_connection(DB_NAME)
    finally:
        cursor.close()
        conn.close()


def main() -> None:
    setup_conn, setup_cursor = create_connection(None)
    try:
        setup_cursor.execute(f"SET GLOBAL enable_experimental_table_ref=1")
        drain(setup_cursor)
        setup_cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        drain(setup_cursor)
        setup_cursor.execute(f"CREATE DATABASE {DB_NAME}")
        drain(setup_cursor)
        setup_cursor.execute(f"USE {DB_NAME}")
        drain(setup_cursor)

        setup_cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        drain(setup_cursor)
        setup_cursor.execute(f"CREATE TABLE {TABLE_NAME} (id BIGINT)")
        drain(setup_cursor)

        setup_cursor.execute(f"INSERT INTO {TABLE_NAME} VALUES (-1)")
        drain(setup_cursor)
        setup_cursor.execute(f"ALTER TABLE {TABLE_NAME} CREATE BRANCH {BRANCH_NAME}")
        drain(setup_cursor)
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

    verify_conn, verify_cursor = create_connection(DB_NAME)
    try:
        expected_branch_rows = 1 + NUM_THREADS * TRANSACTIONS_PER_THREAD * ROWS_PER_TRANSACTION

        verify_cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        main_count = verify_cursor.fetchall()[0][0]

        verify_cursor.execute(
            f"SELECT COUNT(*) AS cnt, COUNT(DISTINCT id) AS uniq FROM {TABLE_NAME}/{BRANCH_NAME}"
        )
        branch_counts = verify_cursor.fetchall()[0]
        branch_total_count, branch_distinct_count = branch_counts[0], branch_counts[1]

        if main_count != 1:
            raise AssertionError(f"Expected main rows=1, got {main_count}")

        if (
            branch_total_count != expected_branch_rows
            or branch_distinct_count != expected_branch_rows
        ):
            raise AssertionError(
                f"Expected branch rows={expected_branch_rows}, got total={branch_total_count}, distinct={branch_distinct_count}"
            )

        verify_cursor.execute(
            f"SELECT id FROM {TABLE_NAME}/{BRANCH_NAME} GROUP BY id HAVING COUNT(*) > 1 LIMIT 1"
        )
        duplicates = verify_cursor.fetchall()
        if duplicates:
            raise AssertionError(f"Found duplicated segments: {duplicates}")

        verify_cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        drain(verify_cursor)
    finally:
        verify_cursor.close()
        verify_conn.close()

    print("Branch transaction snapshot retry looks good")


if __name__ == "__main__":
    main()
