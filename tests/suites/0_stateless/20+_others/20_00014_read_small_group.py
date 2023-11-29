#!/usr/bin/env python3

import os
import sys
import time

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

from client import client

log = None

client1 = client(name="client1>", log=log)

sqls = """
set storage_io_min_bytes_for_seek=0;
SELECT * from @data/ontime_200.parquet;
"""
t1 = time.time_ns()
client1.run(sqls)
t2 = time.time_ns()


sqls = """
set storage_io_min_bytes_for_seek=43;
SELECT * from @data/ontime_200.parquet;
"""
t3 = time.time_ns()
client1.run(sqls)
t4 = time.time_ns()

assert (t4-t3)/(t2-t1) < 0.8 
