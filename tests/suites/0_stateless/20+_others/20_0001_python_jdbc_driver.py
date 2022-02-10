#!/usr/bin/env python3

import os
import sys
import signal
import time
import mysql.connector

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, '../../../helpers'))

from client import client

log = None
# uncomment the line below for debugging
log = sys.stdout

client1 = client(name='client1>', log=log)

sqls = """
DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;
CREATE TABLE IF NOT EXISTS t1(a String, b String, c String, d String, e String, f String, g String, h String) Engine = Memory;
"""

client1.run(sqls)
time.sleep(2)
mydb = mysql.connector.connect(host="127.0.0.1",
                               user="root",
                               passwd="root",
                               port="3307")
mycursor = mydb.cursor()
mycursor.execute("SHOW TABLES FROM db1")
res = mycursor.fetchall()
assert res == [('t1', )]

sql = "DROP DATABASE db1;"
client1.run(sql)
