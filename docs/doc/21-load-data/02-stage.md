---
title: Load Data from Databend Stages
sidebar_label: From Stages
description:
  Load Data from Databend Stages.
---

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/load/load-data-from-stage.png" width="550"/>
</p>

### Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [Deploy Databend With MinIO](/doc/deploy/minio).

### Step 1. Create Stage Object

Execute [CREATE STAGE](/doc/reference/sql/ddl/stage/ddl-create-stage) to create a named internal stage.

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```sql title='mysql>'
CREATE STAGE my_int_stage;
```

```sql title='mysql>'
DESC STAGE my_int_stage;
```

```sql
+--------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
| name         | stage_type | stage_params                                                                                                                                                | copy_options                                  | file_format_options                                                                                                | comment |
+--------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
| my_int_stage | Internal   | StageParams { storage: S3(StageS3Storage { bucket: "", path: "", credentials_aws_key_id: "", credentials_aws_secret_key: "", encryption_master_key: "" }) } | CopyOptions { on_error: None, size_limit: 0 } | FileFormatOptions { format: Csv, skip_header: 0, field_delimiter: ",", record_delimiter: "\n", compression: None } |         |
+--------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
```

### Step 2. Stage the Data Files

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

<Tabs groupId="sample-data">

<TabItem value="csv" label="CSV">

Upload `books.csv` into stages:

```shell title='Request /v1/upload_to_stage' API
curl -H "stage_name:my_int_stage"\
 -F "upload=@./books.csv"\
 -XPUT http://localhost:8081/v1/upload_to_stage
```

```text title='Response'
{"id":"50880048-f397-4d32-994c-ce3d38af430f","stage_name":"my_int_stage","state":"SUCCESS","files":["books.csv"]}
```

:::tip
* http://127.0.0.1:8081/v1/upload_to_stage
  * `127.0.0.1` is `http_handler_host` value in your *databend-query.toml*
  * `8081` is `http_handler_port` value in your *databend-query.toml*

* -F  \"upload=@./books.csv\"
  * Your books.csv file location
:::

</TabItem>

<TabItem value="parquet" label="Parquet">

Upload `books.parquet` into stages:

```shell title='Request /v1/upload_to_stage' API
curl -H "stage_name:my_int_stage"\
 -F "upload=@./books.parquet"\
 -XPUT http://localhost:8081/v1/upload_to_stage
```

```text title='Response'
{"id":"50880048-f397-4d32-994c-ce3d38af430f","stage_name":"my_int_stage","state":"SUCCESS","files":["books.parquet"]}
```

:::tip
* http://127.0.0.1:8081/v1/upload_to_stage
  * `127.0.0.1` is `http_handler_host` value in your *databend-query.toml*
  * `8081` is `http_handler_port` value in your *databend-query.toml*

* -F  \"upload=@./books.parquet\"
  * Your books.csv file location
:::

</TabItem>

</Tabs>


### Step 3. List the Staged Files (Optional)

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```sql title='mysql>'
LIST @my_int_stage;
```

```sql
+---------------+
| file_name     |
+---------------+
| books.csv     |
| books.parquet |
+---------------+
```

### Step 4. Creating Database and Table

```sql title='mysql>'
CREATE DATABASE book_db;
```

```sql title='mysql>'
USE book_db;
```

```sql title='mysql>'
CREATE TABLE books
(
    title VARCHAR,
    author VARCHAR,
    date VARCHAR
);
```

### Step 5. Copy Data into the Target Tables

Execute [COPY](/doc/reference/sql/dml/dml-copy) to load staged files to the target table.

<Tabs groupId="sample-data">

<TabItem value="csv" label="CSV">

```sql title='mysql>'
COPY INTO books FROM '@my_int_stage' files=('books.csv') file_format = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 0);
```


:::tip

* files = ( 'file_name' [ , 'file_name' ... ] )

  Specifies a list of one or more files names (separated by commas) to be loaded.


* file_format
 
| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| record_delimiter | One characters that separate records in an input file. Default `'\n'` | Optional |
| field_delimiter | One characters that separate fields in an input file. Default `','` | Optional |
| skip_header | Number of lines at the start of the file to skip. Default `0` | Optional |
:::

</TabItem>

<TabItem value="parquet" label="Parquet">

```sql title='mysql>'
COPY INTO books FROM '@my_int_stage' files=('books.parquet') file_format = (type = 'Parquet');
```

</TabItem>

</Tabs>


### Step 6. Verify the Loaded Data

```sql
SELECT * FROM Books;
+------------------------------+----------------------+-------+
| title                        | author               | date  |
+------------------------------+----------------------+-------+
| Transaction Processing       |  Jim Gray            |  1992 |
| Readings in Database Systems |  Michael Stonebraker |  2004 |
+------------------------------+----------------------+-------+
```

### Step 7. Congratulations!

You have successfully completed the tutorial.
