---
title: Load Data from Local File
sidebar_label: Load Data from Local File
description:
  Load Data from Local File
---

Using HTTP API `v1/streaming_load` to load data from local file into Databend.
Currently, we only support Csv and Parquet format.

### Before you begin

* **Databend :** Make sure Databend is running and accessible, please see [How to deploy Databend](/doc/category/deploy).

### Step 1. Prepare books.csv

On your local machine, create a text file with the following CSV contents and name it `books.csv`:

```text title="books.csv"
Transaction Processing,Jim Gray,1992
Readings in Database Systems,Michael Stonebraker,2004
```
This CSV file field delimiter is `,` and the record delimiter is `\n`.

### Step 2. Create Database and Table

```shell
mysql -h127.0.0.1 -uroot -P3307
```

```sql title='mysql>'
create database book_db;
```

```sql title='mysql>'
use book_db;
```

```sql title='mysql>'
create table books
(
    title VARCHAR(255),
    author VARCHAR(255),
    date VARCHAR(255)
);
```

### Step 3. Load `books.csv` into Databend

```shell title='Request'
echo curl -H \"insert_sql:insert into book_db.books format CSV\"\
-H \"skip_header:0\" -H \"field_delimiter:','\" -H \"record_delimiter:'\n'\"\
-F  \"upload=@./books.csv\"\
-XPUT http://127.0.0.1:8081/v1/streaming_load|bash
```

```json title='Response'
{
  "id": "f4c557d3-f798-4cea-960a-0ba021dd4646",
  "state": "SUCCESS",
  "stats": {
    "rows": 2,
    "bytes": 157
  },
  "error": null
}
```

:::tip
* http://127.0.0.1:8081/v1/streaming_load
  * `127.0.0.1` is `http_handler_host` value in your *databend-query.toml*
  * `8081` is `http_handler_port` value in your *databend-query.toml*

* -F  \"upload=@./books.csv\"
  * Your books.csv file location
:::


### Step 4. Verify the Loaded Data

```sql title='mysql>'
select * from books;
```

```
+------------------------------+----------------------+-------+
| title                        | author               | date  |
+------------------------------+----------------------+-------+
| Transaction Processing       |  Jim Gray            |  1992 |
| Readings in Database Systems |  Michael Stonebraker |  2004 |
+------------------------------+----------------------+-------+
```

### Step 5. Congratulations!

You have successfully completed the tutorial.