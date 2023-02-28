---
title: Developing with Databend using Python
sidebar_label: Python
description:
   Develop with Databend using Python.
---

Databend offers the following options enabling you to develop applications using the Python programming language and establish connectivity with Databend:

- [databend-py](https://github.com/databendcloud/databend-py): Python Driver, including support for native HTTP interfaces.
- [databend-sqlalchemy](https://github.com/databendcloud/databend-sqlalchemy): Databend SQLAlchemy Dialect.

Click the links above for their installation instructions, examples, and source code on GitHub.

In the following tutorials, you'll learn how to utilize the options above to develop your applications. The tutorials will walk you through creating a SQL user in Databend and then writing Golang code to create a table, insert data, and perform data queries.

## Tutorial: Developing with Databend using databend-py

Before you start, make sure you have successfully installed Databend. For how to install Databend, see [How to deploy Databend](/doc/deploy).

## Step 1. Prepare a SQL User Account

To connect your program to Databend and execute SQL operations, you must provide a SQL user account with appropriate privileges in your code. Create one in Databend if needed, and ensure that the SQL user has only the necessary privileges for security.

This tutorial uses a SQL user named 'user1' with password 'abc123'. As the program will write data into Databend, the user needs ALL privileges. For how to manage SQL users and their privileges, see https://databend.rs/doc/reference/sql/ddl/user.

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
GRANT ALL on *.* TO user1;
```

## Step 2. Write a Python Program

In this step, you'll create a simple Golang program that communicates with Databend. The program will involve tasks such as creating a table, inserting data, and executing data queries.

We will be creating a table named `books` and insert a row, then query it.

### Using databend-py

```shell
pip install databend-py
```

```python title='main.py'
#!/usr/bin/env python3

from databend_py import Client

client = Client('user1:abc123@127.0.0.1', port=8000, secure=False)

# Create database, table.
client.execute("CREATE DATABASE IF NOT EXISTS book_db")
client.execute("USE book_db")
client.execute("CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)")

# Insert new book.
client.execute("INSERT INTO books VALUES('mybook', 'author', '2022')")

# Query.
_, results = client.execute("SELECT * FROM books")
for (title, author, date) in results:
  print("{} {} {}".format(title, author, date))
client.execute('drop table books')
client.execute('drop database book_db')
```

Run `python main.py`:
```text
mybook author 2022
```

### Using databend-sqlalchemy

```shell
pip install databend-sqlalchemy
```

```python title='main.py'
#!/usr/bin/env python3

from databend_sqlalchemy import connector

conn = connector.connect(f"http://user1:abc123@127.0.0.1:8000").cursor()
conn.execute("CREATE DATABASE IF NOT EXISTS book_db")
conn.execute("USE book_db")
conn.execute("CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)")
conn.execute("INSERT INTO books VALUES('mybook', 'author', '2022')")
conn.execute('SELECT * FROM books')
results = conn.fetchall()
for result in results:
    print(result)
conn.execute('drop table books')
conn.execute('drop database book_db')
```

Run `python main.py`:

```text
('mybook', 'author', '2022')
```
