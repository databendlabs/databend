#!/usr/bin/env python3

import sqlalchemy
from sqlalchemy import text
import os

tcp_port = os.getenv("QUERY_MYSQL_HANDLER_PORT")
if tcp_port is None:
    port = "3307"
else:
    port = tcp_port


uri = "mysql+pymysql://root:root@localhost:" + port + "/"
engine = sqlalchemy.create_engine(uri, future=True)
with engine.connect() as conn:
    conn.execute(text("create database if not exists book_db"))
    conn.execute(text("use book_db"))
    conn.execute(
        text(
            "create table if not exists books(title varchar(255), author varchar(255), date varchar(255))"
        )
    )
    conn.execute(text("insert into books values('mybook', 'author', '2022')"))
    results = conn.execute(text("select * from books")).fetchall()
    conn.execute(text("drop database book_db"))
    for result in results:
        print(result)
