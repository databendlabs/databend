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


def get_license():
    return os.getenv("DATABEND_ENTERPRISE_LICENSE")


if __name__ == "__main__":
    with NativeClient(name="client1>") as client1:
        client1.expect(prompt)
        client1.expect("")

        client1.send("set global enterprise_license='{}';".format(get_license()))
        client1.expect(prompt)

        mycursor = mydb.cursor()
        mycursor.execute(
            "select * from system.settings where name='enterprise_license';"
        )
        expect_no_data = mycursor.fetchall()
        print(expect_no_data)
        mycursor = mydb.cursor()
        mycursor.execute("call admin$license_info();")
        license = mycursor.fetchall()
        license[0] = license[0][:5]
        print(license)
