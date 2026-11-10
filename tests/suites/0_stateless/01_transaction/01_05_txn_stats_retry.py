#!/usr/bin/env python3

import os
from contextlib import ExitStack, contextmanager
from uuid import uuid4

import mysql.connector

TABLE_NAME = "txn_stats_retry"
OTHER_TABLE = f"{TABLE_NAME}_other"
HOST = os.getenv("QUERY_MYSQL_HANDLER_HOST", "127.0.0.1")
PORT = int(os.getenv("QUERY_MYSQL_HANDLER_PORT", "3307"))
USER = os.getenv("QUERY_MYSQL_HANDLER_USER", "root")
PASSWORD = os.getenv("QUERY_MYSQL_HANDLER_PASSWORD", "root")


def execute(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


@contextmanager
def sessions():
    with ExitStack() as stack:
        cursors = []
        for _ in range(2):
            conn = mysql.connector.connect(
                host=HOST, port=PORT, user=USER, passwd=PASSWORD, autocommit=False
            )
            stack.callback(conn.close)
            cursor = conn.cursor()
            stack.callback(cursor.close)
            execute(cursor, "SET enable_table_snapshot_stats = 1")
            execute(cursor, "SET enable_auto_analyze = 0")
            execute(cursor, "SET auto_compaction_imperfect_blocks_threshold = 0")
            cursors.append(cursor)
        txn, other = cursors
        try:
            yield txn, other
        finally:
            try:
                execute(txn, "ROLLBACK")
            finally:
                execute(other, f"DROP TABLE IF EXISTS {TABLE_NAME}")
                execute(other, f"DROP TABLE IF EXISTS {OTHER_TABLE}")


def set_collection(cursor, enabled):
    columns = "id" if enabled else ""
    execute(
        cursor,
        f"ALTER TABLE {TABLE_NAME} SET OPTIONS(approx_distinct_columns = '{columns}')",
    )


def create_table(cursor, *, rows=100, collect=True, table=TABLE_NAME):
    execute(cursor, f"DROP TABLE IF EXISTS {table}")
    columns = "id" if collect else ""
    execute(
        cursor, f"CREATE TABLE {table}(id BIGINT) approx_distinct_columns = '{columns}'"
    )
    if rows:
        execute(cursor, f"INSERT INTO {table} SELECT 0 FROM numbers({rows})")
        if collect:
            execute(cursor, f"ANALYZE TABLE {table}")


def check_statistics(cursor, rows, stats_rows, ndv=None, *, table=TABLE_NAME):
    data = execute(cursor, f"SELECT count(*) FROM {table}")
    assert data == [(rows,)], (table, data, rows)
    stats = execute(
        cursor,
        "SELECT stats_row_count, actual_row_count, distinct_count "
        "FROM system.statistics "
        f"WHERE database = current_database() AND table = '{table}' "
        "AND column_name = 'id'",
    )
    assert len(stats) == 1 and stats[0][:2] == (stats_rows, rows), (
        table,
        stats,
        (stats_rows, rows),
    )
    if ndv is not None:
        assert stats[0][2] == ndv, (table, stats, ndv)


def check_analyzed(cursor, rows, ndv):
    execute(cursor, f"ANALYZE TABLE {TABLE_NAME}")
    check_statistics(cursor, rows, rows, ndv)


def passed(name):
    print(f"{name}: statistics after COMMIT retry look good")


def run_insert_case(
    name,
    selects,
    *,
    base_rows=100,
    base_stats=True,
    txn_stats=True,
    concurrent_stats=True,
    ndv=None,
):
    # Each case appends ten rows in A, then five in B. The order forces a
    # version conflict without timing, threads, or retrying the whole transaction.
    with sessions() as (txn, other):
        create_table(other, rows=base_rows, collect=base_stats)
        if txn_stats != base_stats:
            set_collection(other, txn_stats)
        execute(txn, "BEGIN")
        for select in selects:
            execute(txn, f"INSERT INTO {TABLE_NAME} {select}")
        assert execute(txn, f"SELECT count(*) FROM {TABLE_NAME}") == [(base_rows + 10,)]
        if concurrent_stats != txn_stats:
            set_collection(other, concurrent_stats)
        execute(other, f"INSERT INTO {TABLE_NAME} SELECT 0 FROM numbers(5)")
        latest_stats = (base_rows if base_stats else 0) + (5 if concurrent_stats else 0)
        check_statistics(other, base_rows + 5, latest_stats)
        execute(txn, "COMMIT")
        for cursor in [txn, other]:
            check_statistics(
                cursor, base_rows + 15, latest_stats + (10 if txn_stats else 0), ndv
            )
    passed(name)


def run_mixed_insert_case():
    with sessions() as (txn, other):
        create_table(other)
        create_table(other, rows=0, table=OTHER_TABLE)
        execute(txn, "BEGIN")
        execute(
            txn,
            f"INSERT ALL INTO {TABLE_NAME} INTO {OTHER_TABLE} SELECT 1 + number FROM numbers(5)",
        )
        execute(txn, f"INSERT INTO {TABLE_NAME} SELECT 6 + number FROM numbers(5)")
        execute(other, f"INSERT INTO {TABLE_NAME} SELECT 0 FROM numbers(5)")
        execute(txn, "COMMIT")
        for cursor in [txn, other]:
            check_statistics(cursor, 115, 115, 11)
            check_statistics(cursor, 5, 5, 5, table=OTHER_TABLE)
    passed("mixed ordinary and multi-table inserts")


def run_concurrent_analyze_case():
    with sessions() as (txn, other):
        create_table(other, rows=0)
        execute(
            other,
            f"INSERT INTO {TABLE_NAME} SELECT if(number < 50, 0, if(number < 150, 1, 2)) FROM numbers(200)",
        )
        execute(other, f"ANALYZE TABLE {TABLE_NAME}")
        execute(other, f"DELETE FROM {TABLE_NAME} WHERE id = 1")
        check_statistics(other, 100, 200, 3)  # stale HLL still contains the deleted 1
        execute(txn, "BEGIN")
        execute(txn, f"INSERT INTO {TABLE_NAME} SELECT 0 FROM numbers(10)")
        execute(other, f"INSERT INTO {TABLE_NAME} SELECT 0 FROM numbers(5)")
        execute(other, f"ANALYZE TABLE {TABLE_NAME}")
        execute(txn, "COMMIT")
        for cursor in [txn, other]:
            check_statistics(
                cursor, 115, 115, 2
            )  # old value 1 must not be reintroduced
    passed("concurrent ANALYZE removes stale HLL values")


def run_transaction_analyze_case():
    with sessions() as (txn, other):
        create_table(other, collect=False)
        set_collection(other, True)
        execute(txn, "BEGIN")
        execute(txn, f"ANALYZE TABLE {TABLE_NAME}")
        execute(txn, f"INSERT INTO {TABLE_NAME} SELECT 1 + number FROM numbers(10)")
        execute(other, f"INSERT INTO {TABLE_NAME} SELECT 0 FROM numbers(5)")
        execute(txn, "COMMIT")
        # Retry replays the write delta, not A's full refresh of the old 100 rows.
        for cursor in [txn, other]:
            check_statistics(cursor, 115, 15)
        check_analyzed(other, 115, 11)
    passed("transaction ANALYZE keeps append statistics")


def run_insert_delete_case():
    with sessions() as (txn, other):
        create_table(other)
        execute(txn, "BEGIN")
        execute(txn, f"INSERT INTO {TABLE_NAME} SELECT 1 + number FROM numbers(10)")
        execute(txn, f"DELETE FROM {TABLE_NAME} WHERE id BETWEEN 1 AND 6")
        execute(other, f"INSERT INTO {TABLE_NAME} SELECT 0 FROM numbers(5)")
        execute(txn, "COMMIT")
        for cursor in [txn, other]:
            # Deleting six rows does not undo the INSERT's ten collected rows.
            # Do not require an overestimated public NDV: range inference may improve.
            check_statistics(cursor, 109, 115)
            assert execute(cursor, f"SELECT count(DISTINCT id) FROM {TABLE_NAME}") == [
                (5,)
            ]
        check_analyzed(other, 109, 5)
    passed("insert then delete retains accumulated coverage until ANALYZE")


def run_mutation_case(name, statements, expected_data, stats_rows):
    with sessions() as (txn, other):
        create_table(other, rows=0)
        # Disjoint base segments let both sessions' mutations commit successfully.
        execute(other, f"INSERT INTO {TABLE_NAME} SELECT 0 FROM numbers(50)")
        execute(other, f"INSERT INTO {TABLE_NAME} SELECT 1 FROM numbers(50)")
        execute(other, f"ANALYZE TABLE {TABLE_NAME}")
        execute(txn, "BEGIN")
        for statement in statements:
            execute(txn, statement)
        execute(other, f"UPDATE {TABLE_NAME} SET id = 3 WHERE id = 1")
        execute(txn, "COMMIT")
        rows = sum(count for _, count in expected_data)
        for cursor in [txn, other]:
            assert (
                execute(
                    cursor,
                    f"SELECT id, count(*) FROM {TABLE_NAME} GROUP BY id ORDER BY id",
                )
                == expected_data
            )
            check_statistics(cursor, rows, stats_rows)
        check_analyzed(other, rows, len(expected_data))
    passed(name)


def run_single_update_case():
    with sessions() as (txn, other):
        for cursor in [txn, other]:
            execute(cursor, "SET max_threads = 1")
        execute(other, f"DROP TABLE IF EXISTS {TABLE_NAME}")
        execute(
            other,
            f"CREATE TABLE {TABLE_NAME}(id BIGINT) row_per_block = 1000 approx_distinct_columns = 'id'",
        )
        execute(
            other,
            f"INSERT INTO {TABLE_NAME} SELECT if(number = 0, 0, 1) FROM numbers(100)",
        )
        execute(other, f"ANALYZE TABLE {TABLE_NAME}")
        # Every initial block must have multiple rows; the exact number of blocks
        # varies across nodes and is not part of the behavior being tested.
        layout = execute(
            other,
            f"SELECT min(row_count) FROM fuse_block('default', '{TABLE_NAME}')",
        )
        assert layout[0][0] > 1, layout
        execute(txn, "BEGIN")
        execute(txn, f"UPDATE {TABLE_NAME} SET id = 2 WHERE id = 0")
        execute(other, f"INSERT INTO {TABLE_NAME} SELECT 3 FROM numbers(5)")
        execute(txn, "COMMIT")
        for cursor in [txn, other]:
            check_statistics(cursor, 105, 106)
        check_analyzed(other, 105, 3)
    passed("single logical UPDATE does not count the full rewritten block")


def run_deduplicated_insert_case():
    with sessions() as (txn, other):
        create_table(other)
        execute(txn, "BEGIN")
        execute(txn, f"SET deduplicate_label = 'txn_stats_{uuid4().hex}'")
        for _ in range(2):
            execute(txn, f"INSERT INTO {TABLE_NAME} SELECT 1 FROM numbers(3)")
        execute(txn, "SET deduplicate_label = ''")
        # Data reads see buffered rows; system.statistics enumerates committed tables.
        assert execute(txn, f"SELECT count(*) FROM {TABLE_NAME}") == [(103,)]
        execute(other, f"INSERT INTO {TABLE_NAME} SELECT 2 FROM numbers(5)")
        execute(txn, "COMMIT")
        for cursor in [txn, other]:
            check_statistics(cursor, 108, 108, 3)
    passed("deduplicated INSERT records its delta once")


def run_lifecycle_case(mode):
    with sessions() as (txn, other):
        create_table(other)
        execute(txn, "BEGIN")
        execute(txn, f"INSERT INTO {TABLE_NAME} SELECT 9 FROM numbers(5)")
        if mode == "commit":
            execute(txn, "COMMIT")
            check_statistics(other, 105, 105, 2)
        else:
            if mode == "failed statement":
                try:
                    execute(txn, f"INSERT INTO {TABLE_NAME} VALUES ('not-a-bigint')")
                except mysql.connector.Error as error:
                    assert "not-a-bigint" in str(error), error
                else:
                    raise AssertionError("Expected an invalid BIGINT write to fail")
                # COMMIT of an aborted transaction discards the buffered write.
                # Do not mask missing failure cleanup with an explicit ROLLBACK.
                execute(txn, "COMMIT")
            else:
                execute(txn, "ROLLBACK")
            check_statistics(other, 100, 100, 1)
        # Reuse the same session; stale deltas from the first transaction must not
        # inflate this conflict retry or introduce value 9 after rollback/failure.
        execute(txn, "BEGIN")
        execute(txn, f"INSERT INTO {TABLE_NAME} SELECT 2 FROM numbers(10)")
        execute(other, f"INSERT INTO {TABLE_NAME} SELECT 3 FROM numbers(5)")
        execute(txn, "COMMIT")
        rows = 120 if mode == "commit" else 115
        for cursor in [txn, other]:
            check_statistics(cursor, rows, rows, 4 if mode == "commit" else 3)
    passed(f"{mode} clears deltas before the next transaction")


def main():
    inserts = ["SELECT 1 + number FROM numbers(10)"]
    run_insert_case("single insert with low NDV", inserts, ndv=11)
    run_insert_case(
        "multiple inserts",
        ["SELECT 1 + number FROM numbers(5)", "SELECT 6 + number FROM numbers(5)"],
        ndv=11,
    )
    run_mixed_insert_case()
    run_insert_case("statistics disabled in transaction", inserts, txn_stats=False)
    run_insert_case(
        "statistics disabled in concurrent insert", inserts, concurrent_stats=False
    )
    run_insert_case("partially covered table", inserts, base_stats=False)
    run_insert_case(
        "latest snapshot without statistics",
        inserts,
        base_rows=0,
        concurrent_stats=False,
    )
    run_concurrent_analyze_case()
    run_transaction_analyze_case()
    run_insert_delete_case()
    run_mutation_case(
        "repeated updates retain each statement's coverage until ANALYZE",
        [
            f"UPDATE {TABLE_NAME} SET id = 2 WHERE id = 0",
            f"UPDATE {TABLE_NAME} SET id = 4 WHERE id = 2",
        ],
        [(3, 50), (4, 50)],
        250,
    )
    run_mutation_case(
        "DELETE preserves latest coverage until ANALYZE",
        [f"DELETE FROM {TABLE_NAME} WHERE id = 0"],
        [(3, 50)],
        150,
    )
    run_single_update_case()
    run_deduplicated_insert_case()
    for mode in ["rollback", "failed statement", "commit"]:
        run_lifecycle_case(mode)


if __name__ == "__main__":
    main()
