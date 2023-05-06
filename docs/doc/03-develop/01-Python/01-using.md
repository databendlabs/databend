---
title: Using the Python Connector
sidebar_label: Using 
description:
  Using the Python Connector
---

This topic provides a series of examples that illustrate how to use the Databend Connector to perform standard Databend operations such as user login, database and table creation, data insertion/loading, and querying.

The sample code at the end of this topic combines the examples into a single, working Python program.

## Connecting to Databend

Import the `databend-py` module:
```python
import databend_py
```

### Prepare a SQL User Account

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
GRANT ALL on *.* TO user1;
```

### Creating client for databend 

```python
from databend_py import Client
client = Client('user1:abc123@127.0.0.1', port=8000, secure=False)
```

> NOTE:
> 
>Make `secure=False` if your databend client is http and `secure=True` when use https.

## Creating a Database, Table

```python
# Create database, table.
client.execute("CREATE DATABASE IF NOT EXISTS book_db")
client.execute("USE book_db")
client.execute("CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)")
```

## Creating Tables and Inserting Data

```python
# Insert new book.
client.execute("INSERT INTO books VALUES('mybook', 'author', '2022')")
```

## Querying Data

```python
# Query.
try:
    _, results = client.execute("SELECT * FROM books")
    for (title, author, date) in results:
        print("{} {} {}".format(title, author, date))
finally:
    client.disconnect()
```

