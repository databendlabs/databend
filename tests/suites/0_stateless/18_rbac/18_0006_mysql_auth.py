#!/usr/bin/env python3

import os
import mysql.connector
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

log = None

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="root", passwd="", port="3307", connection_timeout=6
    )
    cursor = mydb.cursor()

    cursor.execute("drop user if exists u1;")
    cursor.execute("drop user if exists u2;")
    cursor.execute("drop user if exists u3;")
    cursor.execute("drop user if exists u4;")
    cursor.execute("drop network policy if exists p1;")
    cursor.execute("drop network policy if exists p2;")
    cursor.execute("drop password policy if exists pp1;")
    cursor.execute("create network policy p1 allowed_ip_list=('127.0.0.0/24');")
    cursor.execute(
        "create network policy p2 allowed_ip_list=('127.0.0.0/24') blocked_ip_list=('127.0.0.1');"
    )
    cursor.execute("create password policy pp1 PASSWORD_MAX_RETRIES=2;")
    cursor.execute(
        "create user u1 identified by 'abcDEF123' with set password policy='pp1';"
    )
    cursor.execute(
        "create user u2 identified by 'abc123' with set network policy='p1';"
    )
    cursor.execute(
        "create user u3 identified by 'abc123' with set network policy='p2';"
    )
    cursor.execute(
        "create user u4 identified by 'abc123' with must_change_password = true;"
    )
    cursor.execute("create user u5 identified by 'abc123';")
    cursor.execute("alter user u5 with must_change_password = true;")
except mysql.connector.errors.OperationalError:
    print("root@127.0.0.1 is timeout")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="u1",
        passwd="abcDEF123",
        port="3307",
        connection_timeout=3,
    )
except mysql.connector.errors.OperationalError:
    print("u1 is timeout")

# try with wrong password first time
try:
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="u1",
        passwd="abcDEF1231",
        port="3307",
        connection_timeout=3,
    )
except mysql.connector.errors.OperationalError:
    print("u1 wrong password 1 is timeout")
except mysql.connector.errors.ProgrammingError:
    print("u1 wrong password 1 login fail")

# try with wrong password second time
try:
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="u1",
        passwd="abcDEF1232",
        port="3307",
        connection_timeout=3,
    )
except mysql.connector.errors.OperationalError:
    print("u1 wrong password 2 is timeout")
except mysql.connector.errors.ProgrammingError:
    print("u1 wrong password 2 login fail")

# locked for the wrong passwords twice in a row
try:
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="u1",
        passwd="abcDEF123",
        port="3307",
        connection_timeout=3,
    )
except mysql.connector.errors.OperationalError:
    print("u1 correct password is timeout")
except mysql.connector.errors.ProgrammingError:
    print("u1 correct password locked out")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u2", passwd="abc123", port="3307", connection_timeout=3
    )
except mysql.connector.errors.OperationalError:
    print("u2 is timeout")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="root", passwd="", port="3307", connection_timeout=6
    )
    cursor = mydb.cursor()
    cursor.execute("alter user u2 with disabled=true;")
except mysql.connector.errors.OperationalError:
    print("root@127.0.0.1 is timeout")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u2", passwd="abc123", port="3307", connection_timeout=3
    )
except mysql.connector.errors.OperationalError:
    print("u2 is timeout")
except mysql.connector.errors.ProgrammingError:
    print("AuthenticateFailure: user u2 is disabled. Not allowed to login")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="root", passwd="", port="3307", connection_timeout=6
    )
    cursor = mydb.cursor()
    cursor.execute("alter user u2 with disabled=false;")
except mysql.connector.errors.OperationalError:
    print("root@127.0.0.1 is timeout")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u2", passwd="abc123", port="3307", connection_timeout=3
    )
except mysql.connector.errors.OperationalError:
    print("u2 is timeout")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u3", passwd="abc123", port="3307", connection_timeout=3
    )
except mysql.connector.errors.OperationalError:
    print("u3 is timeout")
except mysql.connector.errors.ProgrammingError:
    print("u3 is blocked by client ip")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u4", passwd="abc123", port="3307", connection_timeout=3
    )
    cursor = mydb.cursor()
    try:
        cursor.execute("select 123;")
    except mysql.connector.errors.DatabaseError as err:
        print("can't execute with error: {}".format(str(err)))

    cursor.execute("alter user user() identified by 'abc456';")
except mysql.connector.errors.OperationalError:
    print("u4 is timeout")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u4", passwd="abc456", port="3307", connection_timeout=3
    )
    cursor = mydb.cursor()
    cursor.execute("select 123;")
except mysql.connector.errors.OperationalError:
    print("u4 is timeout")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u5", passwd="abc123", port="3307", connection_timeout=3
    )
    cursor = mydb.cursor()
    try:
        cursor.execute("select 123;")
    except mysql.connector.errors.DatabaseError as err:
        print("can't execute with error: {}".format(str(err)))

    cursor.execute("alter user user() identified by 'abc456';")
except mysql.connector.errors.OperationalError:
    print("u5 is timeout")

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", user="u5", passwd="abc456", port="3307", connection_timeout=3
    )
    cursor = mydb.cursor()
    cursor.execute("select 123;")
except mysql.connector.errors.OperationalError:
    print("u5 is timeout")
