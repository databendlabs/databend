#!/usr/bin/env python3

import sqlalchemy
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


def recreate_view(con):
    with con.begin() as c:
        c.execute(sqlalchemy.text("DROP VIEW IF EXISTS v_issue_16188"))
    with con.begin() as c:
        c.execute(
            sqlalchemy.text(
                "CREATE OR REPLACE VIEW v_issue_16188 as select a,b from t_issue_16188"
            )
        )


def main():
    tcp_port = os.getenv("QUERY_MYSQL_HANDLER_PORT")
    if tcp_port is None:
        port = "3307"
    else:
        port = tcp_port

    uri = "mysql+pymysql://root:root@localhost:" + port + "/"
    con = sqlalchemy.create_engine(uri, future=True)
    with con.begin() as c:
        c.execute(sqlalchemy.text("DROP TABLE IF EXISTS t_issue_16188"))
        c.execute(
            sqlalchemy.text(
                "CREATE TABLE t_issue_16188 (a int not null, b int not null)"
            )
        )

    with ThreadPoolExecutor(max_workers=64) as executor:
        futures = []
        for _ in range(10):
            futures.append(executor.submit(recreate_view, con))

        for future in as_completed(futures):
            future.result()


if __name__ == "__main__":
    main()
