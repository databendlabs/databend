#!/usr/bin/env python3

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import Random

import mysql.connector
from mysql.connector import errors as mysql_errors


TABLE_NAME = "txn_snapshot_retry_concurrency"
NUM_THREADS = 16
TRANSACTIONS_PER_THREAD = 4
ROWS_PER_TRANSACTION = 2
MAX_RETRIES = 8
RETRY_SLEEP_RANGE = (0.01, 0.05)
VALUE_GAP = 1_000_000

HOST = os.getenv("QUERY_MYSQL_HANDLER_HOST", "127.0.0.1")
PORT = int(os.getenv("QUERY_MYSQL_HANDLER_PORT", "3307"))
USER = os.getenv("QUERY_MYSQL_HANDLER_USER", "root")
PASSWORD = os.getenv("QUERY_MYSQL_HANDLER_PASSWORD", "root")


def create_connection():
    conn = mysql.connector.connect(
        host=HOST, port=PORT, user=USER, passwd=PASSWORD, autocommit=False
    )
    cursor = conn.cursor()
    return conn, cursor


def drain(cursor):
    try:
        cursor.fetchall()
    except mysql_errors.InterfaceError:
        pass


def run_transaction_batch(thread_id: int) -> None:
    conn, cursor = create_connection()
    rng = Random(thread_id)

    try:
        for tx_index in range(TRANSACTIONS_PER_THREAD):
            print(
                f"[thread {thread_id}] start tx {tx_index}",
                file=sys.stderr,
                flush=True,
            )
            base_value = (
                thread_id * VALUE_GAP + tx_index * ROWS_PER_TRANSACTION
            )
            values_clause = ", ".join(
                f"({base_value + offset})" for offset in range(ROWS_PER_TRANSACTION)
            )

            attempts = 0
            while True:
                attempts += 1
                if attempts > 1:
                    print(
                        f"[thread {thread_id}] retry tx {tx_index}, attempt {attempts}",
                        file=sys.stderr,
                        flush=True,
                    )
                try:
                    cursor.execute("BEGIN")
                    drain(cursor)

                    cursor.execute(
                        f"INSERT INTO {TABLE_NAME} VALUES {values_clause}"
                    )
                    drain(cursor)

                    cursor.execute("COMMIT")
                    drain(cursor)
                    print(
                        f"[thread {thread_id}] commit tx {tx_index} succeeded",
                        file=sys.stderr,
                        flush=True,
                    )
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


def main() -> None:
    setup_conn, setup_cursor = create_connection()
    try:
        setup_cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        drain(setup_cursor)
        setup_cursor.execute(f"CREATE TABLE {TABLE_NAME} (id BIGINT)")
        drain(setup_cursor)
    finally:
        setup_cursor.close()
        setup_conn.close()

    print(
        f"Launching {NUM_THREADS} threads, {TRANSACTIONS_PER_THREAD} tx per thread",
        file=sys.stderr,
        flush=True,
    )

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [
            executor.submit(run_transaction_batch, thread_id)
            for thread_id in range(NUM_THREADS)
        ]
        for future in as_completed(futures):
            future.result()
    print("All threads finished", file=sys.stderr, flush=True)

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

        verify_cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        drain(verify_cursor)
    finally:
        verify_cursor.close()
        verify_conn.close()

    print("Transaction snapshot retry looks good")


if __name__ == "__main__":
    main()
