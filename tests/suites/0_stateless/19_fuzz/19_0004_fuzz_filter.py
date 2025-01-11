#!/usr/bin/env python3

import os
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

from client import client

log = None
# uncomment the line below for debugging
log = sys.stdout

client1 = client(name="client1>", log=log)

size = 360
num_predicates = 240
step = size / num_predicates

sql = f"""create or replace table t as select number as a from numbers({size});"""
client1.run(sql)

for i in range(num_predicates):
    sql = f"explain analyze partial select * from t where a >= {int(step * i)};"
    client1.run(sql)
