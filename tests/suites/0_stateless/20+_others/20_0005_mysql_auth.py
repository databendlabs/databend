#!/usr/bin/env python3

import os
import mysql.connector
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

from native_client import NativeClient
from native_client import prompt

log = None

mydb = mysql.connector.connect(host="127.0.0.1", user="root", passwd="", port="3307")

with NativeClient(name="client1>") as client1:
    client1.expect(prompt)
    client1.expect("")
    client1.send("drop user if exists u1;")
    client1.expect(prompt)
    client1.send("drop user if exists u2;")
    client1.expect(prompt)
    client1.send("drop user if exists u3;")
    client1.expect(prompt)
    client1.send("create user u1 identified by 'abc123';")
    client1.expect(prompt)
    client1.send("drop network policy if exists p1;")
    client1.expect(prompt)
    client1.send("drop network policy if exists p2;")
    client1.expect(prompt)
    client1.send("create network policy p1 allowed_ip_list=('127.0.0.0/24');")
    client1.expect(prompt)
    client1.send(
        "create network policy p2 allowed_ip_list=('127.0.0.0/24') blocked_ip_list=('127.0.0.1');"
    )
    client1.expect(prompt)
    client1.send("create user u2 identified by 'abc123' with set network policy='p1';")
    client1.expect(prompt)
    client1.send("create user u3 identified by 'abc123' with set network policy='p2';")
    client1.expect(prompt)

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u1", passwd="abc123", port="3307", connection_timeout=3
    )
except mysql.connector.errors.OperationalError:
    print("u1 is timeout")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u2", passwd="abc123", port="3307", connection_timeout=3
    )
except mysql.connector.errors.OperationalError:
    print("u2 is timeout")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u3", passwd="abc123", port="3307", connection_timeout=3
    )
except mysql.connector.errors.OperationalError:
    print("u3 is timeout")
except mysql.connector.errors.ProgrammingError:
    print("u3 is blocked by client ip")
