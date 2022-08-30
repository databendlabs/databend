---
title: Load Data From Remote Files
sidebar_label: From Remote Files
description:
  Load data from remote files.
---

This tutorial explains how to load data into a table from remote files.

The [COPY INTO `<table>` FROM REMOTE FILES](../30-reference/30-sql/10-dml/dml-copy-into-table-url.md) command allows you to load data into a table from one or more remote files by their URL. The supported file types include CSV, JSON, NDJSON, and PARQUET.

### Before You Begin

Upload the file(s) containing your data to a safe place and get the URL(s). 

This tutorial uses this file as an example: [books.csv](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.csv).

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

The command can also load data from multiple files that are sequentially named. See [COPY INTO `<table>` FROM REMOTE FILES](../30-reference/30-sql/10-dml/dml-copy-into-table-url.md) for details.

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