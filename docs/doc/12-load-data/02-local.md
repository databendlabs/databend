---
title: Tutorial - Load from a Local File
sidebar_label: Tutorial - Load from a Local File
description:
  Load data from local file system.
---

In this tutorial, you will load data from a local sample file into Databend with the [Streaming Load API](../11-integrations/00-api/03-streaming-load.md).

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
mysql -h127.0.0.1 -uroot -P3307
```

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

### Step 2. Load Data into Table

Create and send the API request with the following scripts:

```bash
curl -XPUT 'http://root:@127.0.0.1:8000/v1/streaming_load' -H 'insert_sql: insert into book_db.books file_format = (type = "CSV" skip_header = 0 field_delimiter = "," record_delimiter = "\n")' -F 'upload=@"./books.csv"'
```

Response Example:

```json
{
  "id": "f4c557d3-f798-4cea-960a-0ba021dd4646",
  "state": "SUCCESS",
  "stats": {
    "rows": 2,
    "bytes": 157
  },
  "error": null,
  "files": ["books.csv"]
}
```

### Step 3. Verify Loaded Data

```sql
SELECT * FROM books;

+------------------------------+----------------------+-------+
| title                        | author               | date  |
+------------------------------+----------------------+-------+
| Transaction Processing       |  Jim Gray            |  1992 |
| Readings in Database Systems |  Michael Stonebraker |  2004 |
+------------------------------+----------------------+-------+
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

Create and send the API request with the following scripts:

```bash
curl -XPUT 'http://root:@127.0.0.1:8000/v1/streaming_load' -H 'insert_sql: insert into book_db.bookcomments(title,author,date) file_format = (type = "CSV" skip_header = 0 field_delimiter = "," record_delimiter = "\n")' -F 'upload=@"./books.csv"'
```

Notice that the `insert_sql` part above specifies the columns (title, author, and date) to match the loaded data.

### Step 3. Verify Loaded Data

```sql
SELECT * FROM bookcomments;

+------------------------------+----------------------+----------+--------+
| title                        | author               | comments | date   |
+------------------------------+----------------------+----------+--------+
| Transaction Processing       |  Jim Gray            |          |  1992  |
| Readings in Database Systems |  Michael Stonebraker |          |  2004  |
+------------------------------+----------------------+----------+--------+
```