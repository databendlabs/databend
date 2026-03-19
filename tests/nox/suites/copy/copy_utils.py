def run(conn, sql):
    try:
        if isinstance(sql, tuple):
            (sql, exp) = sql
            res = conn.query_row(sql).values()
            assert res == exp
        else:
            conn.query_row(sql)
    except Exception as ex:
        print(f"'{sql}: {ex}':")
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
