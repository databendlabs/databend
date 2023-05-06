---
title: Using the Databend SQLAlchemy
sidebar_label: Using the Databend SQLAlchemy
description:
  Usage with Databend SQLAlchemy
---
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