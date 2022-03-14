---
title: Load Data from Local File
sidebar_label: Load Data from Local File
description:
  Load Data from Local File
---

Using HTTP API `v1/streaming_load` to load data from local file into Databend.

### Prerequisites

* **Databend :** Make sure Databend is running and accessible, please see [How to deploy Databend](/doc/category/deploy).

### Part 1: Preparing books.csv

On your local machine, create a text file with the following CSV contents and name it `books.csv`:

```text title="books.csv"
Transaction Processing,Jim Gray,1992
Readings in Database Systems,Michael Stonebraker,2004
```
This CSV file field delimiter is `,` and the record delimiter is `\n`.

### Part 2: Creating Database and Table in Databend

```sql
create database book_db;
```

```sql
use book_db;
```

```sql
create table books
(
    title VARCHAR(255),
    author VARCHAR(255),
    date VARCHAR(255)
);
```

### Part 3: Load `books.csv` into Databend

```shell title='Request /v1/streaming_load' API
echo curl -H \"insert_sql:insert into book_db.books format CSV\" -H \"skip_header:0\" -H \"field_delimiter:','\"  -H \"record_delimiter:'\n'\"  -F  \"upload=@./books.csv\"  -XPUT http://127.0.0.1:8081/v1/streaming_load|bash

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


### Part 4: Query the result

```shell title='Request v1/query API'
curl --request POST '127.0.0.1:8081/v1/query/' --header 'Content-Type: application/json' --data-raw '{"sql": "select * from book_db.books"}'
```

```json title='Response'
{
  "id": "f86fa6da-25f1-424c-a6ce-8fb56d0d05e5",
  "schema": {
    "fields": [
      {
        "name": "title",
        "default_expr": null,
        "data_type": {
          "type": "NullableType",
          "inner": {
            "type": "StringType"
          },
          "name": "Nullable(String)"
        }
      },
      {
        "name": "author",
        "default_expr": null,
        "data_type": {
          "type": "NullableType",
          "inner": {
            "type": "StringType"
          },
          "name": "Nullable(String)"
        }
      },
      {
        "name": "date",
        "default_expr": null,
        "data_type": {
          "type": "NullableType",
          "inner": {
            "type": "StringType"
          },
          "name": "Nullable(String)"
        }
      }
    ],
    "metadata": {}
  },
  "data": [
    [
      "Transaction Processing",
      "Jim Gray",
      "1992"
    ],
    [
      "Readings in Database Systems",
      "Michael Stonebraker",
      "2004"
    ]
  ],
  "state": "Succeeded",
  "error": null,
  "stats": {
    "scan_progress": {
      "rows": 2,
      "bytes": 157
    },
    "running_time_ms": 19.9239
  },
  "stats_uri": "/v1/query/f86fa6da-25f1-424c-a6ce-8fb56d0d05e5",
  "final_uri": "/v1/query/f86fa6da-25f1-424c-a6ce-8fb56d0d05e5/kill?delete=true",
  "next_uri": null
}
```
