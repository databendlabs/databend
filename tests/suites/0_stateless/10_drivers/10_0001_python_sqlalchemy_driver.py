#!/usr/bin/env python3

import sqlalchemy
import os

tcp_port = os.getenv("QUERY_MYSQL_HANDLER_PORT")
if tcp_port is None:
    port = "3307"
else:
    port = tcp_port

uri = "mysql+pymysql://root:root@localhost:" + port + "/"
engine = sqlalchemy.create_engine(uri)
conn = engine.connect()
conn.execute("create database if not exists book_db")
conn.execute("use book_db")
conn.execute(
    "create table if not exists books(title varchar(255), author varchar(255), date varchar(255))"
)
conn.execute("insert into books values('mybook', 'author', '2022')")
results = conn.execute('select * from books').fetchall()
for result in results:
    print(result)
conn.execute('drop database book_db')
