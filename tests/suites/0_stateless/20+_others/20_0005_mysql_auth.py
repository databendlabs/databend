#!/usr/bin/env python3

import os
import mysql.connector
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, '../../../helpers'))

from native_client import NativeClient
from native_client import prompt

log = None

mydb = mysql.connector.connect(host="127.0.0.1",
                               user="root",
                               passwd="",
                               port="3307")

with NativeClient(name='client1>') as client1:
    client1.expect(prompt)
    client1.expect('')
    client1.send('drop user if exists u1;')
    client1.expect(prompt)
    client1.send('create user u1 identified by abc123;')
    client1.expect(prompt)

mydb = mysql.connector.connect(host="127.0.0.1",
                               user="u1",
                               passwd="abc123",
                               port="3307")
