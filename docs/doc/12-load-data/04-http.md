---
title: Tutorial - Load from Remote File
sidebar_label: Tutorial - Load from a Remote File
description:
  Load data from remote files.
---

In this tutorial, you will load data from a remote sample file into Databend with the [COPY INTO](../14-sql-commands/10-dml/dml-copy-into-table.md) command.

This tutorial uses this remote sample file: [books.csv](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.csv). The sample contains the following records:

```
Transaction Processing,Jim Gray,1992
Readings in Database Systems,Michael Stonebraker,2004
```

### Step 1. Create Database and Table

```sql
CREATE DATABASE book_db;
USE book_db;

CREATE TABLE books
(
    title VARCHAR,
    author VARCHAR,
    date VARCHAR
);
```

### Step 2. Load Data into the Table

```sql
COPY INTO books FROM 'https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.csv'
    FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 0);
```

:::tip

The command can also load data from multiple files that are sequentially named. See [COPY INTO `<table>`](../14-sql-commands/10-dml/dml-copy-into-table.md) for details.

:::

### Step 3. Verify the Loaded Data

```sql
SELECT * FROM books;
+------------------------------+----------------------+-------+
| title                        | author               | date  |
+------------------------------+----------------------+-------+
| Transaction Processing       |  Jim Gray            |  1992 |
| Readings in Database Systems |  Michael Stonebraker |  2004 |
+------------------------------+----------------------+-------+
```