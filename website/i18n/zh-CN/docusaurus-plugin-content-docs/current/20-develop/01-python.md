---
title: How to Work With Databend in Python
sidebar_label: Python
description: How to work with Databend in Python. ---
---

## Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [How to deploy Databend](/doc/deploy).
* [How to Create User](../30-reference/30-sql/00-ddl/30-user/01-user-create-user.md)
* [How to Grant Privileges to User](../30-reference/30-sql/00-ddl/30-user/10-grant-privileges.md)

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

### Using mysql.connector

```shell
pip install mysql-connector-python
```

```python title='main.py'
#!/usr/bin/env python3
import mysql.connector

cnx = mysql.connector.connect(user='user1', password='abc123',
                              host='127.0.0.1',
                              port = 3307,
                              database='')

# Create database, table.
cursor = cnx.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS book_db")
cursor.execute("USE book_db")
cursor.execute("CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)")

# Insert new book. 
add_book = ("INSERT INTO books "
               "(title, author, date) "
               "VALUES (%s, %s, %s)")
data_book = ('mybook', 'author', '2022')
cursor.execute(add_book, data_book)

# Query.
query = ("SELECT * FROM books")
cursor.execute(query)
for (title, author, date) in cursor:
  print("{} {} {}".format(title, author, date))

cursor.close()
cnx.close()
```

Run `python main.py`:
```text
mybook author 2022
```

### Using sqlalchemy

```shell
pip install sqlalchemy
```

```python title='main.py'
#!/usr/bin/env python3

import sqlalchemy

engine = sqlalchemy.create_engine("mysql+pymysql://user1:abc123@localhost:3307/")
conn = engine.connect()
conn.execute("CREATE DATABASE IF NOT EXISTS book_db")
conn.execute("USE book_db")
conn.execute("CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)")
conn.execute("INSERT INTO books VALUES('mybook', 'author', '2022')")
results = conn.execute('SELECT * FROM books').fetchall()
for result in results:
    print(result)
conn.execute('drop database book_db')

```

Run `python main.py`:
```text
('mybook', 'author', '2022')
```
