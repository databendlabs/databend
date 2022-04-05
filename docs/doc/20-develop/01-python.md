---
title: How to Work with Databend in Python
sidebar_label: python
description:
   How to Work with Databend in Python
---

### Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [How to deploy Databend](/doc/deploy).

### Python

This guideline show how to connect and query to Databend using Python.

We will be creating a table named `books` and insert a row, then query it.

```text
import mysql.connector

cnx = mysql.connector.connect(user='root', password='',
                              host='127.0.0.1',
							  port = 3307,
                              database='book_db')

# Create database, table.
cursor = cnx.cursor()
cursor.execute("create database if not exists book_db")
cursor.execute("use book_db")
cursor.execute("create table if not exists books(title varchar(255), author varchar(255), date varchar(255))")

# Insert new book. 
add_book = ("INSERT INTO books "
               "(title, author, date) "
               "VALUES (%s, %s, %s)")
data_book = ('myname', 'author', '2022')
cursor.execute(add_book, data_book)

# Query.
query = ("select * from books")
cursor.execute(query)
for (title, author, date) in cursor:
  print("{} {} {}".format(title, author, date))

cursor.close()
cnx.close()
```
