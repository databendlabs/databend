---
title: How to Work With Databend in Python
sidebar_label: Python
description:
   How to work with Databend in Python.
---

## Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [How to deploy Databend](/doc/deploy).
* [How to Create User](../14-sql-commands/00-ddl/30-user/01-user-create-user.md)
* [How to Grant Privileges to User](../14-sql-commands/00-ddl/30-user/10-grant-privileges.md)

## Create Databend User

```shell
mysql -h127.0.0.1 -uroot -P3307
```

### Create a User

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

### Grants Privileges

Grants `ALL` privileges to the user `user1`:
```sql
GRANT ALL ON *.* TO user1;
```

## Python

This guideline show how to connect and query to Databend using Python.

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
