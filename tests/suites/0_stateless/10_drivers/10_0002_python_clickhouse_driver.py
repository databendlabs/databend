#!/usr/bin/env python3

import os
import sys
import time
from clickhouse_driver import connect
from clickhouse_driver.dbapi import OperationalError

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
client = connect(host='127.0.0.1', database='db1', user='root', password='', port='9001')
cur = client.cursor()
try:
    cur.execute('INSERT INTO db1.t1(a) VALUES("Test")')
except OperationalError as e:
    assert ('DB:Exception. Unable to get field named "Test". Valid fields: ["a"].' in str(e))
try:
    res=cur.execute('SELECT a FROM db1.t1 WHERE a="Test"')
except Exception as e:
    assert ('DB:Exception. Unknown column Test.' in str(e))
finally:
    sql = "DROP DATABASE db1;"
    client1.run(sql)
