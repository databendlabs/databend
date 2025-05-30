#!/usr/bin/env python3

import os
import mysql.connector
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

from native_client import NativeClient
from native_client import prompt

log = None

# client1 send long query, client mydb fetch the long query and kill it.
# Use to mock in MySQL Client press Ctrl C to intercept a long query.

mydb = mysql.connector.connect(
    host="127.0.0.1", user="root", passwd="root", port="3307"
)

prepare_sqls = [
    "drop table if exists test_explain_analyze_0011_1",
    "create table test_explain_analyze_0011_1 (id int)",
]



with NativeClient(name="client1>") as client1:
    mycursor = mydb.cursor(buffered=True)

    for sql in prepare_sqls:
        mycursor.execute(sql)
        res = mycursor.fetchall()

    for i in range(10):
        mycursor.execute(f"insert into test_explain_analyze_0011_1 values ({i})")
        res = mycursor.fetchall()

    def explain_output(res):
        pruning_fields = ["partitions total", "partitions scanned", "pruning stats"]
        for row in res:
            if any(field in row[0] for field in pruning_fields):
                print(row[0])

    mycursor.execute("""EXPLAIN ANALYZE SELECT * FROM test_explain_analyze_0011_1 a WHERE a.id > 5""")
    explain_analyze_res = mycursor.fetchall()
    explain_output(explain_analyze_res)

    # Same query to test if include pruning cache
    mycursor.execute("""EXPLAIN ANALYZE SELECT * FROM test_explain_analyze_0011_1 a WHERE a.id > 5""")
    explain_analyze_res = mycursor.fetchall()
    explain_output(explain_analyze_res)


    mycursor.execute("""EXPLAIN ANALYZE SELECT * FROM test_explain_analyze_0011_1 a WHERE a.id > 5 AND a.id < 8""")
    explain_analyze_res = mycursor.fetchall()
    explain_output(explain_analyze_res)