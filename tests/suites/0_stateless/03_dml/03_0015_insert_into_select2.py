#!/usr/bin/env python3

import os
import sys
import signal

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
INSERT INTO t1 (a,b,c,d,e,f,g,h) VALUES('1','2','3','4','2021-08-15', '2021-09-15', '2021-08-15 10:00:00', 'string1234'),
                                       ('5','6','7','8','2021-10-15', '2021-11-15', '2021-11-15 10:00:00', 'string5678');

INSERT INTO t1(a,b,c,d,e,f,g,h) select * from t1;
SELECT COUNT(1) = 4 from t1;
DROP DATABASE db1;
"""

client1.run(sqls)
stdout, stderr = client1.run_with_output("select * from system.metrics")
assert stdout is not None
assert stderr is None
