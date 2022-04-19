---
title: How to Work with Databend in Python
sidebar_label: Python
description:
   How to Work with Databend in Python.
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

```sql title='mysql>'
create user user1 identified by 'abc123';
```

### Grants Privileges

Grants `ALL` privileges to the user `user1`:
```sql title='mysql>'
grant all on *.* to 'user1';
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
cursor.execute("create database if not exists book_db")
cursor.execute("use book_db")
cursor.execute("create table if not exists books(title varchar(255), author varchar(255), date varchar(255))")

# Insert new book. 
add_book = ("insert into books "
               "(title, author, date) "
               "values (%s, %s, %s)")
data_book = ('mybook', 'author', '2022')
cursor.execute(add_book, data_book)

# Query.
query = ("SELECT * from books")
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
conn.execute("create database if not exists book_db")
conn.execute("use book_db")
conn.execute("create table if not exists books(title varchar(255), author varchar(255), date varchar(255))")
conn.execute("insert into books values('mybook', 'author', '2022')")
results = conn.execute('SELECT * from books').fetchall()
for result in results:
    print(result)
conn.execute('drop database book_db')

```

Run `python main.py`:
```text
('mybook', 'author', '2022')
```
