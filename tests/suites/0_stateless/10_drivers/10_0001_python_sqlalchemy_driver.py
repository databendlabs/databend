#!/usr/bin/env python3

import sqlalchemy

engine = sqlalchemy.create_engine("mysql+pymysql://root:root@localhost:3307/")
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
