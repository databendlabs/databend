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
    sql = "CREATE TABLE test.table_name_{i}(id int(5) comment 't\est', c1 int comment 'sss') comment='test'".format(i=i)
    cursor.execute(sql)
db.commit()

def test_speed(cursor, name):
    sql = "select count() from system."+ name +" where database='test'"
    cursor.execute(sql)
    start = datetime.now()
    cursor.execute(sql)
    execute = (datetime.now()-start)
    # In ci 0.068s, in local debug build cost 0.03s
    # But of ci is vary, we set the exception value to 0.1s
    # if not modify system table but the test failed, we should up the value again.
    if execute.total_seconds() > 0.1:
        print("Err: query system.%s in one db cost exception, cost: %s"%(name, execute))
    else:
        print("normal")

test_speed(cursor, "tables")
test_speed(cursor, "columns")
db.close()
