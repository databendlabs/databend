from pprint import pformat


def _assert_eq(sql, actual, expected):
    if actual != expected:
        raise AssertionError(
            "query result mismatch\n"
            f"sql: {sql}\n"
            f"expected: {pformat(expected)}\n"
            f"actual:   {pformat(actual)}"
        )


def run(conn, sql):
    try:
        if isinstance(sql, tuple):
            (sql, exp) = sql
            if isinstance(exp, tuple):
                res = conn.query_row(sql).values()
                _assert_eq(sql, res, exp)
            else:
                res = [row.values() for row in conn.query_iter(sql)]
                _assert_eq(sql, res, exp)
        else:
            conn.query_row(sql)
    except Exception as ex:
        print(f"'{sql}' fail: {ex}")
        raise ex

def run_all(conn, sqls):
    for sql in sqls:
        run(conn, sql)

def prepare(conn, name):
    conn.exec(f"create or replace stage {name}")
    conn.exec(f"create or replace database {name}")
    conn.exec(f"use {name}")

def clean_up(conn, name):
    conn.exec(f"drop stage if exists {name}")
    conn.exec(f"drop database if exists {name}")
