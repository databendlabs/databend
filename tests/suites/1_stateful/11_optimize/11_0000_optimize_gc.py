#!/usr/bin/env python3

import os
import time
import mysql.connector
import sys
import signal
from multiprocessing import Process

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

from native_client import NativeClient
from native_client import prompt

log = None

# client1 send long query, client mydb fetch the long query and kill it.
# Use to mock in MySQL Client press Ctrl C to intercept a long query.

mydb = mysql.connector.connect(
    host="127.0.0.1", user="root", passwd="root", port="3307"
)

def insert_data(name):
    mycursor = mydb.cursor()
    value = 1
    while True:
        sql = "insert into table gc_test values(%d);" % value
        mycursor.execute(sql)
        value +=1

def compact_data(name):
    mycursor = mydb.cursor()
    mycursor.execute('optimize table gc_test all;')

if __name__ == '__main__':
    with NativeClient(name="client1>") as client1:
        client1.expect(prompt)

        client1.send("drop table if exists gc_test;")
        client1.expect(prompt)

        client1.send("create table gc_test(a int);");
        client1.expect(prompt)

        p = Process(target=insert_data, args=('insert_data',))
        p.start()

        time.sleep(1)
        #kill the insert_data child process randomly
        p.kill()
        time.sleep(0.1)

        p = Process(target=compact_data, args=('compact_data',))
        p.start()

        time.sleep(0.1)
        #kill the compact_data child process randomly
        p.kill()
        time.sleep(0.1)

        mycursor = mydb.cursor()
        mycursor.execute('select a from gc_test order by a;')
        old_datas = mycursor.fetchall()

        client1.send("optimize table gc_test gc;");
        client1.expect(prompt)

        mycursor.execute('select a from gc_test order by a;')
        datas = mycursor.fetchall()

        if old_datas != datas:
            print("gc lose data: %s : %s" % (old_datas, datas))
        else:
            print("gc success")


