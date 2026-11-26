#!/usr/bin/env python3

# A MySQL connection authenticates once, but reloads its user before each query: a REVOKE,
# disabling the user or DROP USER takes effect on an open connection without reconnecting,
# a user created again under the same name is not the connection's user,
# and a password changed on the connection lifts MUST_CHANGE_PASSWORD there.

import os
import re

import mysql.connector

HOST = os.getenv("QUERY_MYSQL_HANDLER_HOST", "127.0.0.1")
PORT = int(os.getenv("QUERY_MYSQL_HANDLER_PORT", "3307"))
USER = os.getenv("QUERY_MYSQL_HANDLER_USER", "root")
PASSWORD = os.getenv("QUERY_MYSQL_HANDLER_PASSWORD", "root")

NAME = "reload_user_18_0028"
NAME_MUST_CHANGE = "reload_must_change_18_0028"
TABLE = "reload_db_18_0028.t"


def connect(user, password):
    return mysql.connector.connect(host=HOST, port=PORT, user=user, passwd=password)


def run(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def query(label, cursor, sql):
    try:
        rows = run(cursor, sql)
        print(f"{label}: {rows[0][0] if rows else 'ok'}")
    except mysql.connector.Error as e:
        found = re.search(r"(\w+)\. Code: (\d+)", str(e))
        print(
            f"{label}: {found.group(1)} {found.group(2)}" if found else f"{label}: {e}"
        )


root_connection = connect(USER, PASSWORD)
root = root_connection.cursor()
for sql in [
    f"DROP USER IF EXISTS {NAME}",
    f"DROP USER IF EXISTS {NAME_MUST_CHANGE}",
    "DROP DATABASE IF EXISTS reload_db_18_0028",
    "DROP ROLE IF EXISTS reload_reader_18_0028",
    "DROP ROLE IF EXISTS reload_limited_18_0028",
    "CREATE DATABASE reload_db_18_0028",
    f"CREATE TABLE {TABLE}(a INT)",
    f"INSERT INTO {TABLE} VALUES (1)",
    "CREATE ROLE reload_reader_18_0028",
    "CREATE ROLE reload_limited_18_0028",
    f"GRANT SELECT ON {TABLE} TO ROLE reload_reader_18_0028",
    f"CREATE USER {NAME} IDENTIFIED BY 'password'",
    f"GRANT ROLE reload_reader_18_0028 TO {NAME}",
    f"GRANT ROLE reload_limited_18_0028 TO {NAME}",
]:
    run(root, sql)

user_connection = connect(NAME, "password")
user = user_connection.cursor()
connection_id = run(user, "SELECT connection_id()")[0][0]


def same_connection():
    assert run(user, "SELECT connection_id()")[0][0] == connection_id


print("== revoked secondary role")
run(user, "SET ROLE reload_limited_18_0028")
run(user, "SET SECONDARY ROLES reload_reader_18_0028")
query("granted", user, f"SELECT count(*) FROM {TABLE}")
run(root, f"REVOKE ROLE reload_reader_18_0028 FROM {NAME}")
query("revoked", user, f"SELECT count(*) FROM {TABLE}")
run(root, f"GRANT ROLE reload_reader_18_0028 TO {NAME}")
query("granted again", user, f"SELECT count(*) FROM {TABLE}")
same_connection()

print("== revoked current role")
run(user, "SET ROLE reload_reader_18_0028")
run(user, "SET SECONDARY ROLES NONE")
query("granted", user, f"SELECT count(*) FROM {TABLE}")
run(root, f"REVOKE ROLE reload_reader_18_0028 FROM {NAME}")
query("revoked", user, f"SELECT count(*) FROM {TABLE}")
query("current role", user, "SELECT current_role()")
same_connection()

print("== revoked account_admin")
run(root, f"GRANT ROLE account_admin TO {NAME}")
run(user, "SET ROLE account_admin")
query("granted", user, "CREATE TABLE reload_db_18_0028.t2(a INT)")
run(root, f"REVOKE ROLE account_admin FROM {NAME}")
query("revoked", user, "DROP TABLE reload_db_18_0028.t2")
same_connection()

print("== password changed on the connection")
run(
    root,
    f"CREATE USER {NAME_MUST_CHANGE} IDENTIFIED BY 'password' WITH MUST_CHANGE_PASSWORD = TRUE",
)
must_change_connection = connect(NAME_MUST_CHANGE, "password")
must_change = must_change_connection.cursor()
query("must change", must_change, "SELECT 1")
query("changed", must_change, "ALTER USER USER() IDENTIFIED BY 'new-password'")
query("after change", must_change, "SELECT 1")
must_change_connection.close()

print("== disabled user")
run(root, f"ALTER USER {NAME} WITH DISABLED = TRUE")
query("disabled", user, "SELECT 1")
run(root, f"ALTER USER {NAME} WITH DISABLED = FALSE")
query("enabled again", user, "SELECT 1")
same_connection()

print("== dropped user")
run(root, f"DROP USER {NAME}")
query("dropped", user, "SELECT 1")

print("== user created again under the same name")
run(root, f"CREATE USER {NAME} IDENTIFIED BY 'other-password'")
run(root, f"GRANT SELECT ON {TABLE} TO {NAME}")
query("old connection", user, f"SELECT count(*) FROM {TABLE}")
new_connection = connect(NAME, "other-password")
query("new connection", new_connection.cursor(), f"SELECT count(*) FROM {TABLE}")
new_connection.close()

for sql in [
    f"DROP USER IF EXISTS {NAME}",
    f"DROP USER IF EXISTS {NAME_MUST_CHANGE}",
    "DROP DATABASE IF EXISTS reload_db_18_0028",
    "DROP ROLE IF EXISTS reload_reader_18_0028",
    "DROP ROLE IF EXISTS reload_limited_18_0028",
]:
    run(root, sql)
