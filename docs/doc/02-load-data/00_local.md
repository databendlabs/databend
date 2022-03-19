---
title: Load Data from Local File System
sidebar_label: Load Data from Local File System
description:
  Load Data from Local File System
---

Using HTTP API `v1/streaming_load` to load data from local file into Databend.
Currently, we only support CSV and Parquet file format.

### Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [How to deploy Databend](/doc/category/deploy).

### Step 1. Data Files for Loading

Download the sample data file(Choose CSV or Parquet), the file contains two records:
```text
Transaction Processing,Jim Gray,1992
Readings in Database Systems,Michael Stonebraker,2004
```

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="sample-data">

<TabItem value="csv" label="CSV">

Download [books.csv](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.csv)

</TabItem>

<TabItem value="parquet" label="Parquet">

Download [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet)

</TabItem>

</Tabs>

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

### Step 3. Load Data into the Target Tables

<Tabs groupId="load-data">

<TabItem value="csv" label="CSV">

```shell title='Request'
echo curl -H \"insert_sql:insert into book_db.books format CSV\" -H \"skip_header:0\" -H \"field_delimiter:','\" -H \"record_delimiter:'\n'\" -F  \"upload=@./books.csv\" -XPUT http://127.0.0.1:8081/v1/streaming_load|bash
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

* skip_header: Number of lines at the start of the file to skip
* field_delimiter: One character that separate fields 
* record_delimiter: One character that separate records
* -F  \"upload=@./books.csv\"
  * Your books.csv file location
:::

</TabItem>

<TabItem value="parquet" label="Parquet">

```shell title='Request'
echo curl -H \"insert_sql:insert into book_db.books format Parquet\" -F  \"upload=@./books.parquet\" -XPUT http://127.0.0.1:8081/v1/streaming_load|bash
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

* -F  \"upload=@./books.parquet\"
  * Your books.parquet file location
:::

</TabItem>

</Tabs>


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