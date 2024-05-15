#!/usr/bin/env python3

import os
import mysql.connector
import sys
from datetime import datetime

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

from native_client import NativeClient  # NOQA
from native_client import prompt  # NOQA

log = None

# client1 send long query, client mydb fetch the long query and kill it.
# Use to mock in MySQL Client press Ctrl C to intercept a long query.

mydb = mysql.connector.connect(
    host="127.0.0.1", user="root", passwd="root", port="3307"
)


def get_license():
    return os.getenv("QUERY_DATABEND_ENTERPRISE_LICENSE")


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

        issuer = license[0][0]
        type_ = license[0][1]
        organization = license[0][2]
        issue_at = license[0][3]
        expire_at = license[0][4]
        features = license[0][6]
        now = datetime.utcnow()

        if now < issue_at or now > expire_at:
            print(
                f"License is invalid: issuer: {issuer}, type: {type_}, "
                f"org: {organization}, issue_at: {issue_at}, expire_at: {expire_at}, features: {features}"
            )
            sys.exit(1)
        print(features)
        mycursor = mydb.cursor()
        mycursor.execute(
            "select count(*) from system.processes where type = 'FlightSQL';"
        )
        all_process = mycursor.fetchall()
        print(all_process)
