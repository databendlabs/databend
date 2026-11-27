#!/usr/bin/env python3

# A user lists only its own sessions in system.processes and SHOW PROCESSLIST, as KILL only
# lets it kill its own queries. With SUPER, it lists all sessions of its tenant, never those
# of another tenant. Checked over the MySQL and the HTTP handler.

import base64
import json
import os
import urllib.request

import mysql.connector

HOST = os.getenv("QUERY_MYSQL_HANDLER_HOST", "127.0.0.1")
PORT = int(os.getenv("QUERY_MYSQL_HANDLER_PORT", "3307"))
HTTP_PORT = int(os.getenv("QUERY_HTTP_HANDLER_PORT", "8000"))
USER = os.getenv("QUERY_MYSQL_HANDLER_USER", "root")
PASSWORD = os.getenv("QUERY_MYSQL_HANDLER_PASSWORD", "root")

NAME = "processes_reader_18_0030"


def connect(user, password):
    return mysql.connector.connect(host=HOST, port=PORT, user=user, passwd=password)


def run(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def http_query(user, password, sql):
    """Runs `sql` through the HTTP handler and returns all its rows."""
    base = f"http://{HOST}:{HTTP_PORT}"
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
    body = json.dumps({"sql": sql}).encode()
    request = urllib.request.Request(f"{base}/v1/query", data=body, headers=headers)
    rows = []
    while True:
        with urllib.request.urlopen(request) as response:
            result = json.load(response)
        if result.get("error"):
            raise RuntimeError(result["error"])
        rows.extend(result.get("data") or [])
        if not result.get("next_uri"):
            return rows
        request = urllib.request.Request(base + result["next_uri"], headers=headers)


root_connection = connect(USER, PASSWORD)
root = root_connection.cursor()
run(root, f"DROP USER IF EXISTS {NAME}")
run(root, f"CREATE USER {NAME} IDENTIFIED BY 'password'")
root_id = run(root, "SELECT connection_id()")[0][0]

# An admin session of another tenant on the same node.
other_tenant_connection = connect(USER, PASSWORD)
other_tenant = other_tenant_connection.cursor()
run(other_tenant, "SET sandbox_tenant = 'processes_tenant_18_0030'")
other_tenant_id = run(other_tenant, "SELECT connection_id()")[0][0]


def report(label, source, own_id, ids):
    print(
        f"{label}, {source}: own {own_id in ids}, admin {root_id in ids}, "
        f"other tenant {other_tenant_id in ids}"
    )


# Sessions are told apart by `id`, the value of `connection_id()`.
def print_listed(label, cursor):
    own_id = run(cursor, "SELECT connection_id()")[0][0]
    rows = run(cursor, "SELECT id FROM system.processes")
    report(label, "MySQL system.processes", own_id, {row[0] for row in rows})
    rows = run(cursor, "SHOW PROCESSLIST")
    column = [description[0] for description in cursor.description].index("id")
    report(label, "MySQL SHOW PROCESSLIST", own_id, {row[column] for row in rows})
    # An HTTP request is a session of its own, listed while it runs.
    rows = http_query(
        NAME, "password", "SELECT connection_id(), id FROM system.processes"
    )
    report(label, "HTTP system.processes", rows[0][0], {row[1] for row in rows})


user_connection = connect(NAME, "password")
print_listed("no SUPER", user_connection.cursor())
user_connection.close()

run(root, f"GRANT SUPER ON *.* TO {NAME}")
user_connection = connect(NAME, "password")
print_listed("SUPER", user_connection.cursor())
user_connection.close()

other_tenant_connection.close()
run(root, f"DROP USER {NAME}")
