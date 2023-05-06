---
title: Developing with Databend using Python
sidebar_label: Python
description:
  Develop with Databend using Python.
---

## Installing
To install the latest Python Connector for Databend, use:

```shell
pip install databend_py
```

The source code for the Python driver is available on [GitHub](https://github.com/databendcloud/databend-py).

## Prerequisites
Requires Python version 3.5 or later.

## Using
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

## Using the Databend SQLAlchemy

Databend SQLAlchemy runs on the top of the Databend Connector for Python as a dialect to bridge a Databend database and SQLAlchemy applications.

## Prerequisites
### Databend Connector for Python
The only requirement for Databend SQLAlchemy is the Databend Connector for Python; however, the connector does not need to be installed because installing Databend SQLAlchemy automatically installs the connector.

## Install databend-sqlalchemy.

```shell
pip install databend-sqlalchemy
```


## Sample code

```python title='main.py'
#!/usr/bin/env python3

from databend_sqlalchemy import connector

conn = connector.connect(f"http://user1:abc123@127.0.0.1:8000")
cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS book_db")
cursor.execute("USE book_db")
cursor.execute("CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)")
cursor.execute("INSERT INTO books VALUES('mybook', 'author', '2022')")
cursor.execute('SELECT * FROM books')
results = cursor.fetchall()
for result in results:
    print(result)
cursor.execute('drop table books')
cursor.execute('drop database book_db')

# Close Connect.
cursor.close()
```
## Databend-specific Parameters and Behavior

### Connection Parameters

```python
#!/usr/bin/env python3

from sqlalchemy import create_engine, text
engine = create_engine(
            f"databend://{username}:{password}@{host_port}/{database_name}?secure=false"
        )
connection = engine.connect()
result = connection.execute(text("SELECT 1"))
connection.close()
engine.dispose()
```
Where:
- `username` is the login name for your Databend user.
- `password` is the password for your Databend user.
- `host_port` is the databend host and port.
- `database_name` is the initial database  for the Databend session.
- `secure` means the databend host is http or https.