# #!/usr/bin/env python3
#
# import os
# import time
# import mysql.connector
# import sys
# import signal
#
# CURDIR = os.path.dirname(os.path.realpath(__file__))
# sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))
#
# from native_client import NativeClient
# from native_client import prompt
#
# log = None
#
# # client1 send long query, client mydb fetch the long query and kill it.
# # Use to mock in MySQL Client press Ctrl C to intercept a long query.
#
# mydb = mysql.connector.connect(
#     host="127.0.0.1", user="root", passwd="root", port="3307"
# )
#
# with NativeClient(name="client1>") as client1:
#     client1.expect(prompt)
#     client1.expect("")
#
#     client1.send(
#         "SELECT max(number), sum(number) FROM numbers_mt(100000000000) GROUP BY number % 3, number % 4, number % 5 LIMIT 10;"
#     )
#     time.sleep(0.2)
#
#     mycursor = mydb.cursor()
#
#     client1.send(
#         "select avg(number) from numbers(100000000000);"
#     )
#     time.sleep(0.2)
#
#
#     mycursor.execute(
#         "SHOW PROCESSLIST;"
#     )
#     res = mycursor.fetchone()
#
#     print(res)
#
