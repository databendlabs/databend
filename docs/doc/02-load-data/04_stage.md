---
title: Load Data from Databend Stages
sidebar_label: Load Data from Stages
description:
  Load Data from Databend Stages
---


### Before you begin

* **Databend :** Make sure Databend is running and accessible, please see [Deploy Databend With MinIO](/doc/deploy/minio).

### Step 1. Create Stage Object

Execute [CREATE STAGE](/doc/reference/sql/ddl/stage/ddl-create-stage) to create a named internal stage.

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```sql title='mysql>'
create stage my_int_stage;
```

```sql title='mysql>'
desc stage my_int_stage;
```

```sql
+--------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
| name         | stage_type | stage_params                                                                                                                                                | copy_options                                  | file_format_options                                                                                                | comment |
+--------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
| my_int_stage | Internal   | StageParams { storage: S3(StageS3Storage { bucket: "", path: "", credentials_aws_key_id: "", credentials_aws_secret_key: "", encryption_master_key: "" }) } | CopyOptions { on_error: None, size_limit: 0 } | FileFormatOptions { format: Csv, skip_header: 0, field_delimiter: ",", record_delimiter: "\n", compression: None } |         |
+--------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+
```

### Step 2. Stage the Data Files

On your local machine, create a text file with the following CSV contents and name it `books.csv`:

```text title="books.csv"
Transaction Processing,Jim Gray,1992
Readings in Database Systems,Michael Stonebraker,2004
```
This CSV file field delimiter is `,` and the record delimiter is `\n`.

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


### Step 3. List the Staged Files (Optional)

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```sql title='mysql>'
list @my_int_stage;
```

```text
+-----------+
| file_name |
+-----------+
| books.csv |
+-----------+
```

### Step 4. Creating Database and Table

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

### Step 5. Copy Data into the Target Tables

```sql title='mysql>'
copy into books from '@my_int_stage' file_format = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 0);
```

### Step 6. Verify the Loaded Data

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

### Step 7. Congratulations!

You have successfully completed the tutorial.
