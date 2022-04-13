---
title: How to Work with Databend in Python
sidebar_label: Python
description:
   How to Work with Databend in Python.
---

### Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [How to deploy Databend](/doc/deploy).

### Create a user and grant privileges

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```sql title='mysql>'
create user 'databend'@'%' IDENTIFIED BY 'password123';
```

```sql title='mysql>'
grant all privileges on *.* TO 'databend'@'%';
```

### Python

This guideline show how to connect and query to Databend using Python.

We will be creating a table named `books` and insert a row, then query it.

### Using mysql.connector
`pip install mysql-connector-python`

```python
#!/usr/bin/env python3
import mysql.connector

cnx = mysql.connector.connect(user='databend', password='password123',
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
query = ("select * from books")
cursor.execute(query)
for (title, author, date) in cursor:
  print("{} {} {}".format(title, author, date))

cursor.close()
cnx.close()
```

### Using sqlalchemy

`pip install sqlalchemy`

```python
#!/usr/bin/env python3

import sqlalchemy

engine = sqlalchemy.create_engine("mysql+pymysql://databend:password123@localhost:3307/")
conn = engine.connect()
conn.execute("create database if not exists book_db")
conn.execute("use book_db")
conn.execute("create table if not exists books(title varchar(255), author varchar(255), date varchar(255))")
conn.execute("insert into books values('mybook', 'author', '2022')")
results = conn.execute('select * from books').fetchall()
for result in results:
    print(result)
conn.execute('drop database book_db')

```
