#!/usr/bin/env python3

import os
import time
import mysql.connector
import sys
import signal

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

from native_client import NativeClient
from native_client import prompt

log = None

mydb = mysql.connector.connect(
    host="127.0.0.1", user="root", passwd="root", port="3307"
)

with NativeClient(name="client1>") as client1:
    client1.expect(prompt)
    client1.expect("")

    client1.send(
        "SELECT max(number), sum(number) FROM numbers_mt(100000000000) GROUP BY number % 3, number % 4, number % 5 LIMIT 10;"
    )
    time.sleep(0.5)

    mycursor = mydb.cursor()
    mycursor.execute(
        "SELECT id FROM system.processes WHERE extra_info LIKE '%SELECT max(number)%' AND extra_info NOT LIKE '%system.processes%';"
    )
    res = mycursor.fetchone()

    adjust_priority_query = "set priority high '" + str(res[0]) + "';"
    mycursor.execute(adjust_priority_query)
    time.sleep(0.2)

    kill_query = "kill query '" + str(res[0]) + "';"
    mycursor.execute(kill_query)
    time.sleep(0.1)

    client1.expect(prompt)

