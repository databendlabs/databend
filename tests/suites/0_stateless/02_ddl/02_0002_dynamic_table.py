#!/usr/bin/env python3
"""Dynamic table source privileges and SHOW CREATE round trips over MySQL."""

import os

import mysql.connector


def connect(user="root", password=""):
    return mysql.connector.connect(
        host=os.getenv("QUERY_MYSQL_HANDLER_HOST", "127.0.0.1"),
        port=int(os.getenv("QUERY_MYSQL_HANDLER_PORT", "3307")),
        user=user,
        password=password,
    )


def query(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall() if cursor.with_rows else []


def denied(connection, sql):
    try:
        query(connection, sql)
    except mysql.connector.Error as error:
        assert "1063" in str(error), str(error)
    else:
        raise AssertionError(f"Expected permission denial: {sql}")


with connect() as root:
    query(root, "DROP DATABASE IF EXISTS dt_review")
    query(root, "DROP DATABASE IF EXISTS dt_private")
    query(root, 'DROP DATABASE IF EXISTS "dt`quoted"')
    query(root, "DROP USER IF EXISTS dt_review_user")
    query(root, "DROP ROLE IF EXISTS dt_review_role")
    try:
        query(root, "CREATE DATABASE dt_review")
        query(root, "CREATE DATABASE dt_private")
        query(root, "CREATE TABLE dt_private.src(id INT NOT NULL)")
        query(root, "INSERT INTO dt_private.src VALUES (1)")
        query(root, "CREATE ROLE dt_review_role")
        query(
            root,
            "CREATE USER dt_review_user IDENTIFIED BY 'password' "
            "WITH DEFAULT_ROLE='dt_review_role'",
        )
        query(root, "GRANT ROLE dt_review_role TO dt_review_user")
        query(root, "GRANT CREATE ON dt_review.* TO ROLE dt_review_role")
        create = "CREATE DYNAMIC TABLE dt_review.dt AS SELECT id FROM dt_private.src"
        with connect("dt_review_user", "password") as user:
            denied(user, create)
        assert query(
            root,
            "SELECT count(*) FROM system.tables "
            "WHERE database='dt_review' AND name='dt'",
        ) == [(0,)]

        query(root, "GRANT SELECT ON dt_private.src TO ROLE dt_review_role")
        with connect("dt_review_user", "password") as user:
            query(user, create)
        query(root, "REVOKE SELECT ON dt_private.src FROM ROLE dt_review_role")
        query(root, "INSERT INTO dt_private.src VALUES (2)")
        with connect("dt_review_user", "password") as user:
            assert query(user, "SELECT id FROM dt_review.dt") == [(1,)]
            denied(user, "REFRESH DYNAMIC TABLE dt_review.dt")
        assert query(root, "SELECT id FROM dt_review.dt") == [(1,)]
        query(root, "GRANT SELECT ON dt_private.src TO ROLE dt_review_role")
        with connect("dt_review_user", "password") as user:
            query(user, "REFRESH DYNAMIC TABLE dt_review.dt")
            assert query(user, "SELECT id FROM dt_review.dt ORDER BY id") == [
                (1,),
                (2,),
            ]
        print("source privileges checked on create and refresh")

        # Preserve explicit target casts, comments, transient status, clustering and
        # user options, while escaping both database and table identifiers.
        query(root, "SET hide_options_in_show_create_table=0")
        query(root, 'CREATE DATABASE "dt`quoted"')
        name = '"dt`quoted"."result`table"'
        query(
            root,
            f"CREATE TRANSIENT DYNAMIC TABLE {name} "
            "(value BIGINT NOT NULL COMMENT 'target column') CLUSTER BY (value) "
            "COMPRESSION='lz4' STORAGE_FORMAT='parquet' COMMENT='derived data' "
            "AS SELECT id FROM dt_private.src",
        )
        definition = query(root, f"SHOW CREATE TABLE {name}")[0][1]
        assert (
            "CREATE TRANSIENT DYNAMIC TABLE `dt``quoted`.`result``table`" in definition
        )
        assert "BIGINT" in definition and "target column" in definition
        assert "COMPRESSION='lz4'" in definition and "derived data" in definition
        assert "source_table_ids" not in definition.lower()
        assert "snapshot_location" not in definition.lower()
        before = query(root, f"DESC {name}")
        query(root, f"DROP TABLE {name}")
        query(root, definition)
        assert query(root, f"DESC {name}") == before
        assert query(root, f"SHOW CREATE TABLE {name}")[0][1] == definition
        assert query(root, f"SELECT value FROM {name} ORDER BY value") == [(1,), (2,)]
        query(root, f"REFRESH DYNAMIC TABLE {name}")
        print("SHOW CREATE preserves schema, options and quoted identifiers")
    finally:
        query(root, 'DROP DATABASE IF EXISTS "dt`quoted"')
        query(root, "DROP DATABASE IF EXISTS dt_review")
        query(root, "DROP DATABASE IF EXISTS dt_private")
        query(root, "DROP USER IF EXISTS dt_review_user")
        query(root, "DROP ROLE IF EXISTS dt_review_role")
