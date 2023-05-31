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

        client1.send("set global enterprise_license='{}';".format(get_license()))
        client1.expect(prompt)

        client1.send("drop MASKING POLICY if exists mask")

        mycursor = mydb.cursor()
        mycursor.execute(
            "CREATE MASKING POLICY mask AS (val STRING,num int) RETURN STRING -> CASE WHEN current_role() IN ('ANALYST') THEN VAL ELSE '*********'END comment = 'this is a masking policy';"
        )

        mycursor = mydb.cursor()
        mycursor.execute("desc masking policy mask;")
        mask = mycursor.fetchall()[0]
        print(mask[0], mask[2], mask[3], mask[4], mask[5])

        mycursor.execute("drop MASKING POLICY if exists mask")
        mycursor.execute("desc masking policy mask;")
        mask = mycursor.fetchall()
        print(mask)
