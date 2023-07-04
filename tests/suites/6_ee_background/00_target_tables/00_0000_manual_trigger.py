#!/usr/bin/env python3

import os
import time
import mysql.connector
import sys
import signal
from multiprocessing import Process
from datetime import datetime

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


def get_license():
    return os.getenv("DATABEND_ENTERPRISE_LICENSE")


if __name__ == "__main__":
    with NativeClient(name="client1>") as client1:
        client1.expect(prompt)
        client1.expect("")
        client1.send("set global enterprise_license='{}';".format(get_license()))
        client1.expect(prompt)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        mycursor = mydb.cursor()
        mycursor.execute(
            "select creator, trigger from system.background_tasks where created_on > TO_TIMESTAMP('{}') AND trigger IS NOT NULL;".format(
                current_time
            )
        )
        expect_no_data = mycursor.fetchall()
        print(expect_no_data)
        mycursor = mydb.cursor()
        mycursor.execute("create table if not exists default.target1(i int);")
        expect_no_data = mycursor.fetchall()
        print(expect_no_data)
        mycursor = mydb.cursor()
        mycursor.execute("call system$execute_background_job('test_tenant-compactor-job');")
        expect_no_data = mycursor.fetchall()
        print(expect_no_data)
        # time.sleep(3)
        # mycursor = mydb.cursor()
        # mycursor.execute(
        #     "select creator, trigger from system.background_tasks where created_on > TO_TIMESTAMP('{}') AND trigger IS NOT NULL;".format(current_time)
        # )
        # trigger = mycursor.fetchall()
        # print(trigger)
        # mycursor = mydb.cursor()
        # mycursor.execute("create table if not exists target2(i int);")
        # expect_no_data = mycursor.fetchall()
        # print(expect_no_data)
        # mycursor.execute("call system$execute_background_job('test_tenant-compactor-job');")
        # expect_no_data = mycursor.fetchall()
        # print(expect_no_data)
        # time.sleep(3)
        # mycursor = mydb.cursor()
        # # target table once designated, will be compacted by the background job automatically, and will NOT be filtered by the trigger
        # mycursor.execute(
        #     "select creator, trigger from system.background_tasks where created_on > TO_TIMESTAMP('{}') AND trigger IS NOT NULL;".format(current_time)
        # )
        # trigger = mycursor.fetchall()
        # print(trigger)
