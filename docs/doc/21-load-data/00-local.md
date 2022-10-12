---
title: Load Data From Local File System
sidebar_label: From Local File System
description:
  Load data from local file system.
---

![image](../../public/img/load/load-data-from-local-fs.png)

Databend offers the [Streaming Load API](#streaming-load-api) that lets you load data from your local files.

## Streaming Load API

The Streaming Load API is used to read data from your local files in one of the following formats and loads it into Databend:

- CSV (UTF-8 encoded)
- TSV
- JSON
- NDJSON
- Parquet

### API Request Format

To create a request with the Streaming Load API, follow the format below:

```bash
curl -H "<parameter>:<value>"  [-H "<parameter>:<value>"...] -F "upload=@<file_location>" -XPUT http://<user_name>:[password]@<http_handler_host>:<http_handler_port>/v1/streaming_load
```
### Explaining Argument `-H`

The request usually includes many occurrences of the argument `-H` and each is followed by one of the following parameters to tell Databend how to handle the file you're loading data from:

| Parameter               | Values                                                                                                                                                                                                                                                                                                     | Supported Formats         | Examples                                       |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|------------------------------------------------|
| insert_sql              | [INSERT_statement] + format [file_format]                                                                                                                                                                                                                                                                  | All                       | -H "insert_sql: insert into ontime format CSV" |
| format_skip_header      | Tells Databend how many lines at the beginning of the file to skip for header.<br /> 0 (default): No lines to skip;<br /> 1: Skip the first line;<br /> N: Skip the first N lines.                                                                                                                               | CSV / TSV / JSON / NDJSON | -H "format_skip_header: 1"                     |
| format_compression      | Tells Databend the compression format of the file.<br /> NONE (default): Do NOT decompress the file;<br /> AUTO: Automatically decompress the file by suffix;<br />  You can also use one of these values to explicitly specify the compression format: GZIP \| BZ2 \| BROTLI \| ZSTD \|  DEFALTE \| RAW_DEFLATE. | CSV / TSV / JSON / NDJSON | -H "format_compression:auto"                   |
| format_field_delimiter  | Tells Databend the characters used in the file to separate fields.<br /> Default for CSV files: `,`.<br /> Default for TSV files: `\t`.                                                                                                                                                                        | CSV / TSV                 | -H "format_field_delimiter:,"                  |
| format_record_delimiter | Tells Databend the new line characters used in the file to separate records.<br />  Default: `\n`.                                                                                                                                                                                                           | CSV / TSV                 | -H "format_recorder_delimiter:\n"              |
| format_quote_char       | Tells Databend the quote characters for strings in CSV file.<br /> Default: `:`.                                                                                                                                                                                                                             | CSV                       |                                                |

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
curl -XPUT 'http://root:@127.0.0.1:8081/v1/streaming_load' -H 'insert_sql: insert into book_db.books format CSV' -H 'skip_header: 0' -H 'field_delimiter: ,' -H 'record_delimiter: \n' -F 'upload=@"./books.csv"'
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
curl -XPUT 'http://root:@127.0.0.1:8081/v1/streaming_load' -H 'insert_sql: insert into book_db.bookcomments(title,author,date)format CSV' -H 'skip_header: 0' -H 'field_delimiter: ,' -H 'record_delimiter: \n' -F 'upload=@"./books.csv"'
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