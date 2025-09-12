#!/usr/bin/env python3

import mysql.connector

# https://github.com/databendlabs/databend/issues/18705
if __name__ == "__main__":
    # Session 1:
    # Insert into empty table
    mdb = mysql.connector.connect(
        host="127.0.0.1", user="root", passwd="root", port="3307"
    )
    mycursor = mdb.cursor()
    mycursor.execute("create or replace table t_18705(c int)")
    mycursor.fetchall()
    mycursor.execute("begin")
    mycursor.fetchall()
    mycursor.execute("insert into t_18705 values (1)")
    mycursor.fetchall()

    # Session 2:
    # Alter table in another session, so that the new table after alter operation will still be empty
    mydb_alter_tbl = mysql.connector.connect(
        host="127.0.0.1", user="root", passwd="root", port="3307"
    )
    mycursor_alter_tbl = mydb_alter_tbl.cursor()
    mycursor_alter_tbl.execute(
        "alter table t_18705 SET OPTIONS (block_per_segment = 500)"
    )
    mycursor_alter_tbl.fetchall()

    # Session 1:
    # Try commit the txn in session one
    mycursor.execute("commit")
    mycursor.fetchall()

    # Will not reach here, if `commit` failed
    print("Looks good")
