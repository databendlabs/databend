---
title: Loading from Stage
---

Databend enables you to easily import data from files uploaded to either the user stage or an internal/external stage. To do so, you can first upload the files to a stage with [PRESIGN](../../14-sql-commands/00-ddl/80-presign/presign.md), and then employ the [COPY INTO](../../14-sql-commands/10-dml/dml-copy-into-table.md) command to load the data from the staged file. Please note that the files must be in a format supported by Databend, otherwise the data cannot be imported. For more information on the file formats supported by Databend, see [Input & Output File Formats](../../13-sql-reference/50-file-format-options.md).

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

Upload sample file using the Presigned URL method:

```sql
PRESIGN UPLOAD @~/books.parquet;

Name   |Value                                                                                                                                                                                                                                                                                                                                                       |
-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
method |PUT                                                                                                                                                                                                                                                                                                                                                         |
headers|{"host":"s3.us-east-2.amazonaws.com"}                                                                                                                                                                                                                                                                                                                       |
url    |https://s3.us-east-2.amazonaws.com/databend-toronto/stage/user/root/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIASTQNLUZWP2UY2HSN%2F20230627%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230627T153448Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=84f1c280bff52f33c1914d64b2091d19650ad4882137013601fc44d26b607933|
```
```shell
curl -X PUT -T books.parquet "https://s3.us-east-2.amazonaws.com/databend-toronto/stage/user/root/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIASTQNLUZWP2UY2HSN%2F20230627%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230627T153448Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=84f1c280bff52f33c1914d64b2091d19650ad4882137013601fc44d26b607933"
```

Check the staged file:

```sql
LIST @~;

name         |size|md5                               |last_modified                |creator|
-------------+----+----------------------------------+-----------------------------+-------+
books.parquet| 998|"88432bf90aadb79073682988b39d461c"|2023-06-27 16:03:51.000 +0000|       |
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

name             |stage_type|number_of_files|creator   |comment|
-----------------+----------+---------------+----------+-------+
my_internal_stage|Internal  |              0|'root'@'%'|       |
```

### Step 2: Upload Sample File

Upload sample file using the Presigned URL method:

```sql
CREATE STAGE my_internal_stage;
```
```sql
PRESIGN UPLOAD @my_internal_stage/books.parquet;

Name   |Value                                                                                                                                                                                                                                                                                                                                                                        |
-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
method |PUT                                                                                                                                                                                                                                                                                                                                                                          |
headers|{"host":"s3.us-east-2.amazonaws.com"}                                                                                                                                                                                                                                                                                                                                        |
url    |https://s3.us-east-2.amazonaws.com/databend-toronto/stage/internal/my_internal_stage/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIASTQNLUZWP2UY2HSN%2F20230628%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230628T022951Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=9cfcdf3b3554280211f88629d60358c6d6e6a5e49cd83146f1daea7dfe37f5c1|
```

```shell
curl -X PUT -T books.parquet "https://s3.us-east-2.amazonaws.com/databend-toronto/stage/internal/my_internal_stage/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIASTQNLUZWP2UY2HSN%2F20230628%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230628T022951Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=9cfcdf3b3554280211f88629d60358c6d6e6a5e49cd83146f1daea7dfe37f5c1"
```

Check the staged file:

```sql
LIST @my_internal_stage;

name                               |size  |md5                               |last_modified                |creator|
-----------------------------------+------+----------------------------------+-----------------------------+-------+
books.parquet                      |   998|"88432bf90aadb79073682988b39d461c"|2023-06-28 02:32:15.000 +0000|       |
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
my_external_stage|External  |               |'root'@'%'|       |
```

### Step 2: Upload Sample File

Upload sample file using the Presigned URL method:

```sql
PRESIGN UPLOAD @my_external_stage/books.parquet;

Name   |Value                                                                                                                                                                                                                                                                                                      |
-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
method |PUT                                                                                                                                                                                                                                                                                                        |
headers|{"host":"127.0.0.1:9000"}                                                                                                                                                                                                                                                                                  |
url    |http://127.0.0.1:9000/databend/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ROOTUSER%2F20230628%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230628T040959Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=697d608750fdcfe4a0b739b409cd340272201351023baa823382bf8c3718a4bd|
```
```shell
curl -X PUT -T books.parquet "http://127.0.0.1:9000/databend/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ROOTUSER%2F20230628%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230628T040959Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=697d608750fdcfe4a0b739b409cd340272201351023baa823382bf8c3718a4bd"
```

Check the staged file:

```sql
LIST @my_external_stage;

name         |size|md5                               |last_modified                |creator|
-------------+----+----------------------------------+-----------------------------+-------+
books.parquet| 998|"88432bf90aadb79073682988b39d461c"|2023-06-28 04:13:15.178 +0000|       |
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