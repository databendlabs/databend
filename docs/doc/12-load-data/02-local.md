---
title: Loading from a Local File
sidebar_label: Loading from a Local File
description:
  Load data from local file system.
---

In this tutorial, you will load data from a local sample file into Databend with the [BendSQL](../11-integrations/30-access-tool/01-bendsql.md).

## Tutorial 1 - Load from a CSV File

This tutorial takes a CSV file as an example, showing how to load data into Databend from a local file.

### Before You Begin

Download the sample CSV file [books.csv](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.csv). The sample contains the following records:

```
Transaction Processing,Jim Gray,1992
Readings in Database Systems,Michael Stonebraker,2004
```

### Step 1. Create Database and Table

```shell
> bendsql
Welcome to BendSQL.
Trying connect to localhost:8000 as user root.
Connected to DatabendQuery v1.1.2-nightly-8ade21e4669e0a2cc100615247705feacdf76c5b(rust-1.70.0-nightly-2023-04-15T16:08:52.195357424Z)

root@localhost> CREATE DATABASE book_db;
Processed in (0.020 sec)

root@localhost> use book_db;

USE book_db

0 row in 0.020 sec. Processed 0 rows, 0B (0 rows/s, 0B/s)

root@localhost> CREATE TABLE books
(
    title VARCHAR,
    author VARCHAR,
    date VARCHAR
);
Processed in (0.029 sec)

root@localhost>
```

### Step 2. Load Data into Table

Send loading data request with the following command:

```shell
> bendsql --query='INSERT INTO book_db.books VALUES;' --format=csv --data=@books.csv --progress
==> Stream Loaded books.csv:
    Written 2 (24.29 rows/s), 157B (1.86 KiB/s)
```

### Step 3. Verify Loaded Data

```shell
> echo "SELECT * FROM books;" | bendsql --database book_db
┌─────────────────────────────────────────────────────────────┐
│             title            │        author       │  date  │
│            String            │        String       │ String │
├──────────────────────────────┼─────────────────────┼────────┤
│ Transaction Processing       │ Jim Gray            │ 1992   │
│ Readings in Database Systems │ Michael Stonebraker │ 2004   │
└─────────────────────────────────────────────────────────────┘
```

## Tutorial 2 - Load into Specified Columns

In [Tutorial 1](#tutorial-1---load-from-a-csv-file), you created a table containing three columns that exactly match the data in the sample file. The Streaming Load API also allows you to load data into specified columns of a table in Databend, so the table does not need to have the same columns as the data to be loaded as long as the specified columns can match. This tutorial shows how to do that.

### Before You Begin

Before you start this tutorial, make sure you have completed [Tutorial 1](#tutorial-1---load-from-a-csv-file).

### Step 1. Create Table

Create a table including an extra column named "comments" compared to the table "books":

```sql
CREATE TABLE bookcomments
(
    title VARCHAR,
    author VARCHAR,
    comments VARCHAR,
    date VARCHAR
);
```

### Step 2. Load Data into Table

Send loading data request with the following command:

```bash
> bendsql --query='INSERT INTO book_db.bookcomments(title,author,date) VALUES;' --format=csv --data=@books.csv --progress
==> Stream Loaded books.csv:
    Written 2 (23.23 rows/s), 221B (2.51 KiB/s)
```

Notice that the `query` part above specifies the columns (title, author, and date) to match the loaded data.

### Step 3. Verify Loaded Data

```sql
SELECT * FROM bookcomments;

┌────────────────────────────────────────────────────────────────────────┐
│             title            │        author       │ comments │  date  │
│            String            │        String       │  String  │ String │
├──────────────────────────────┼─────────────────────┼──────────┼────────┤
│ Transaction Processing       │ Jim Gray            │          │ 1992   │
│ Readings in Database Systems │ Michael Stonebraker │          │ 2004   │
└────────────────────────────────────────────────────────────────────────┘

2 rows in 0.033 sec. Processed 2 rows, 2B (60.42 rows/s, 7.14 KiB/s)
```
