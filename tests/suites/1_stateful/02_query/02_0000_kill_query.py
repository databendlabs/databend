#!/usr/bin/env python3

import os
import time
import mysql.connector
import sys
import signal

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, '../../../helpers'))

from native_client import NativeClient
from native_client import prompt

log = None

with NativeClient(name='client1>') as client1:
    client1.expect(prompt)

    client1.send('SET enable_new_processor_framework=0;')
    client1.expect('')
    client1.send('SELECT * FROM numbers(999999999);')
    client1.expect(prompt)

with NativeClient(name='client2>') as client2:
    client2.expect(prompt)

    # client1 send long query, client select the long query and kill it
    # Use to mock in MySQL Client press Ctrl C to intercept a long query.
    mydb = mysql.connector.connect(host="127.0.0.1",
                                   user="root",
                                   passwd="root",
                                   port="3307")
    mycursor = mydb.cursor()
    mycursor.execute(
        "SELECT mysql_connection_id FROM system.processes WHERE extra_info LIKE '%select * from numbers(999999999)%'"
    )
    res = mycursor.fetchone()
    kill_query = 'kill query ' + str(res[0]) + ';'
    client2.send(kill_query)
    client2.expect(prompt)
    time.sleep(5)
    mycursor.execute(
        "SELECT * FROM system.processes WHERE extra_info LIKE '%select * from numbers(999999999)%' AND extra_info NOT LIKE '%system.processes%'"
    )
    res = mycursor.fetchone()
    assert res is None
