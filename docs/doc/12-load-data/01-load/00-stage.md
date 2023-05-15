---
title: Loading from Stage
---

Databend enables you to easily import data from files uploaded to either the user stage or an internal/external stage. To do so, you can first use the [File Upload API](../../11-integrations/00-api/10-put-to-stage.md) to upload the files to a stage, and then employ the [COPY INTO](../../14-sql-commands/10-dml/dml-copy-into-table.md) command to load the data from the staged file. Please note that the files must be in a format supported by Databend, otherwise the data cannot be imported. For more information on the file formats supported by Databend, see [Input & Output File Formats](../../13-sql-reference/50-file-format-options.md).

![image](/img/load/load-data-from-stage.jpeg)

The following tutorials offer a detailed, step-by-step guide to help you effectively navigate the process of loading data from files in a stage.

## Before You Begin

Before you start, make sure you have completed the following tasks:

- Download and save the sample file [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet) to a local folder. The file contains two records:

```text
Transaction Processing,Jim Gray,1992
Readings in Database Systems,Michael Stonebraker,2004
```

- Create a table with the following SQL statements in Databend:

```sql
USE default;
CREATE TABLE books
(
    title VARCHAR,
    author VARCHAR,
    date VARCHAR
);
```

## Tutorial 1: Loading from User Stage

Follow this tutorial to upload the sample file to the user stage and load data from the staged file into Databend.

### Step 1: Upload Sample File

1. Use cURL to make a request to the [File Upload API](../../11-integrations/00-api/10-put-to-stage.md):

```shell title='Upload to User Stage:'
curl -u root: -H "stage_name:~" -F "upload=@books.parquet" -XPUT "http://localhost:8000/v1/upload_to_stage"
```

```shell title='Response:'
{"id":"6e45fb1e-562c-496b-8500-f0b103cae3c7","stage_name":"~","state":"SUCCESS","files":["books.parquet"]}    
```

2. Check the uploaded file:

```sql
LIST @~;

---
name         |size|md5                               |last_modified                |creator|
-------------+----+----------------------------------+-----------------------------+-------+
books.parquet| 998|"88432bf90aadb79073682988b39d461c"|2023-04-24 14:45:26.753 +0000|       |
```

### Step 2. Copy Data into Table

1. Load data into the target table with the [COPY INTO](../../14-sql-commands/10-dml/dml-copy-into-table.md) command:

```sql
COPY INTO books FROM @~ files=('books.parquet') FILE_FORMAT = (TYPE = PARQUET);
```

2. Check the loaded data:

```sql
SELECT * FROM books;

---
title                       |author             |date|
----------------------------+-------------------+----+
Transaction Processing      |Jim Gray           |1992|
Readings in Database Systems|Michael Stonebraker|2004|
```

## Tutorial 2: Loading from Internal Stage

Follow this tutorial to upload the sample file to an internal stage and load data from the staged file into Databend.

### Step 1. Create an Internal Stage

1. Create an internal stage with the [CREATE STAGE](../../14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md) command:

```sql
CREATE STAGE my_internal_stage;
```
2. Check the created stage:

```sql
SHOW STAGES;

name             |stage_type|number_of_files|creator           |comment|
-----------------+----------+---------------+------------------+-------+
my_internal_stage|Internal  |              0|'root'@'127.0.0.1'|       |
```

### Step 2: Upload Sample File

1. Use cURL to make a request to the [File Upload API](../../11-integrations/00-api/10-put-to-stage.md):

```shell title='Upload to Internal Stage:'
curl -u root: -H "stage_name:my_internal_stage" -F "upload=@books.parquet" -XPUT "http://localhost:8000/v1/upload_to_stage"
```

```shell title='Response:'
{"id":"2828649a-1eee-4feb-9222-68cefd8cd096","stage_name":"my_internal_stage","state":"SUCCESS","files":["books.parquet"]}
```

2. Check the uploaded file:

```sql
LIST @my_internal_stage;

---
name         |size|md5                               |last_modified                |creator|
-------------+----+----------------------------------+-----------------------------+-------+
books.parquet| 998|"88432bf90aadb79073682988b39d461c"|2023-04-24 15:17:44.205 +0000|       |
```

### Step 3. Copy Data into Table

1. Load data into the target table with the [COPY INTO](../../14-sql-commands/10-dml/dml-copy-into-table.md) command:

```sql
COPY INTO books FROM @my_internal_stage files=('books.parquet') FILE_FORMAT = (TYPE = PARQUET);
```
2. Check the loaded data:

```sql
SELECT * FROM books;

---
title                       |author             |date|
----------------------------+-------------------+----+
Transaction Processing      |Jim Gray           |1992|
Readings in Database Systems|Michael Stonebraker|2004|
```

## Tutorial 3: Loading from External Stage

Follow this tutorial to upload the sample file to an external stage and load data from the staged file into Databend.

### Step 1. Create an External Stage

1. Create an external stage with the [CREATE STAGE](../../14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md) command:

```sql
CREATE STAGE my_external_stage url = 's3://databend' CONNECTION =(ENDPOINT_URL= 'http://127.0.0.1:9000' aws_key_id='ROOTUSER' aws_secret_key='CHANGEME123');
```

2. Check the created stage:

```sql
SHOW STAGES;

name             |stage_type|number_of_files|creator           |comment|
-----------------+----------+---------------+------------------+-------+
my_external_stage|External  |               |'root'@'127.0.0.1'|       |
```

### Step 2: Upload Sample File

1. Use cURL to make a request to the [File Upload API](../../11-integrations/00-api/10-put-to-stage.md):

```shell title='Upload to External Stage:'
curl  -u root: -H "stage_name:my_external_stage" -F "upload=@books.parquet" -XPUT "http://127.0.0.1:8000/v1/upload_to_stage"
```

```shell title='Response:'
{"id":"a21844fc-4c06-4b95-85a0-d57c28b9a142","stage_name":"my_external_stage","state":"SUCCESS","files":["books.parquet"]}
```

2. Check the uploaded file:

```sql
LIST @my_external_stage;

---
name         |size|md5                               |last_modified                |creator|
-------------+----+----------------------------------+-----------------------------+-------+
books.parquet| 998|"88432bf90aadb79073682988b39d461c"|2023-04-24 15:47:40.727 +0000|       |
```

### Step 3. Copy Data into Table

1. Load data into the target table with the [COPY INTO](../../14-sql-commands/10-dml/dml-copy-into-table.md) command:

```sql
COPY INTO books FROM @my_external_stage files=('books.parquet') FILE_FORMAT = (TYPE = PARQUET);
```
2. Check the loaded data:

```sql
SELECT * FROM books;

---
title                       |author             |date|
----------------------------+-------------------+----+
Transaction Processing      |Jim Gray           |1992|
Readings in Database Systems|Michael Stonebraker|2004|
```