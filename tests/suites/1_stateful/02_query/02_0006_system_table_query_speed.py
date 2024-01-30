#!/usr/bin/env python3

import os
import sys
from datetime import datetime
import mysql.connector


db = mysql.connector.connect(host="127.0.0.1", user="root", passwd="root", port="3307", buffered=True)
cursor = db.cursor()
cursor.execute("drop database if exists test;")
cursor.execute("create database test;")
for i in range(100):
    sql = 'CREATE TABLE test.table_name_{i}(id int(5))'.format(i=i)
    cursor.execute(sql)
db.commit()

cursor.execute("select count() from system.tables where database='test'")
start = datetime.now()
cursor.execute("select count() from system.tables where database='test'")
execute = (datetime.now()-start)
if execute.total_seconds() > 0.05:
    print("Err: query 100 tables in one db cost exception, cost: ", execute)
else:
    print("normal")

db.close()
