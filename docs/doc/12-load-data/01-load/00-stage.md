---
title: Loading from Stage
---

Databend enables you to easily import data from files uploaded to either the user stage or an internal/external stage. To do so, you can first upload the files to a stage using [BendSQL](../../13-sql-clients/01-bendsql.md), and then employ the [COPY INTO](/14-sql-commands/10-dml/dml-copy-into-table.md) command to load the data from the staged file. Please note that the files must be in a format supported by Databend, otherwise the data cannot be imported. For more information on the file formats supported by Databend, see [Input & Output File Formats](/13-sql-reference/50-file-format-options.md).

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

1. Upload the sample file using [BendSQL](../../13-sql-clients/01-bendsql.md):

```sql
root@localhost:8000/default> PUT fs:///Users/eric/Documents/books.parquet @~

PUT fs:///Users/eric/Documents/books.parquet @~

┌───────────────────────────────────────────────┐
│                 file                │  status │
│                String               │  String │
├─────────────────────────────────────┼─────────┤
│ /Users/eric/Documents/books.parquet │ SUCCESS │
└───────────────────────────────────────────────┘
```

2. Verify the staged file:

```sql
LIST @~;

name         |size|md5                               |last_modified                |creator|
-------------+----+----------------------------------+-----------------------------+-------+
books.parquet| 998|"88432bf90aadb79073682988b39d461c"|2023-06-27 16:03:51.000 +0000|       |
```

### Step 2. Copy Data into Table

1. Load data into the target table with the [COPY INTO](/14-sql-commands/10-dml/dml-copy-into-table.md) command:

```sql
COPY INTO books FROM @~ files=('books.parquet') FILE_FORMAT = (TYPE = PARQUET);
```

2. Verify the loaded data:

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

1. Create an internal stage with the [CREATE STAGE](/14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md) command:

```sql
CREATE STAGE my_internal_stage;
```
2. Verify the created stage:

```sql
SHOW STAGES;

name             |stage_type|number_of_files|creator   |comment|
-----------------+----------+---------------+----------+-------+
my_internal_stage|Internal  |              0|'root'@'%'|       |
```

### Step 2: Upload Sample File

1. Upload the sample file using [BendSQL](../../13-sql-clients/01-bendsql.md):

```sql
root@localhost:8000/default> CREATE STAGE my_internal_stage;

CREATE STAGE my_internal_stage

0 row written in 0.049 sec. Processed 0 rows, 0 B (0 rows/s, 0 B/s)

root@localhost:8000/default> PUT fs:///Users/eric/Documents/books.parquet @my_internal_stage

PUT fs:///Users/eric/Documents/books.parquet @my_internal_stage

┌───────────────────────────────────────────────┐
│                 file                │  status │
│                String               │  String │
├─────────────────────────────────────┼─────────┤
│ /Users/eric/Documents/books.parquet │ SUCCESS │
└───────────────────────────────────────────────┘
```

2. Verify the staged file:

```sql
LIST @my_internal_stage;

name                               |size  |md5                               |last_modified                |creator|
-----------------------------------+------+----------------------------------+-----------------------------+-------+
books.parquet                      |   998|"88432bf90aadb79073682988b39d461c"|2023-06-28 02:32:15.000 +0000|       |
```

### Step 3. Copy Data into Table

1. Load data into the target table with the [COPY INTO](/14-sql-commands/10-dml/dml-copy-into-table.md) command:

```sql
COPY INTO books FROM @my_internal_stage files=('books.parquet') FILE_FORMAT = (TYPE = PARQUET);
```
2. Verify the loaded data:

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

1. Create an external stage with the [CREATE STAGE](/14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md) command:

```sql
CREATE STAGE my_external_stage url = 's3://databend' CONNECTION =(ENDPOINT_URL= 'http://127.0.0.1:9000' aws_key_id='ROOTUSER' aws_secret_key='CHANGEME123');
```

2. Verify the created stage:

```sql
SHOW STAGES;

name             |stage_type|number_of_files|creator           |comment|
-----------------+----------+---------------+------------------+-------+
my_external_stage|External  |               |'root'@'%'|       |
```

### Step 2: Upload Sample File

1. Upload the sample file using [BendSQL](../../13-sql-clients/01-bendsql.md):

```sql
root@localhost:8000/default> PUT fs:///Users/eric/Documents/books.parquet @my_external_stage

PUT fs:///Users/eric/Documents/books.parquet @my_external_stage

┌───────────────────────────────────────────────┐
│                 file                │  status │
│                String               │  String │
├─────────────────────────────────────┼─────────┤
│ /Users/eric/Documents/books.parquet │ SUCCESS │
└───────────────────────────────────────────────┘
```

2. Verify the staged file:

```sql
LIST @my_external_stage;

name         |size|md5                               |last_modified                |creator|
-------------+----+----------------------------------+-----------------------------+-------+
books.parquet| 998|"88432bf90aadb79073682988b39d461c"|2023-06-28 04:13:15.178 +0000|       |
```

### Step 3. Copy Data into Table

1. Load data into the target table with the [COPY INTO](/14-sql-commands/10-dml/dml-copy-into-table.md) command:

```sql
COPY INTO books FROM @my_external_stage files=('books.parquet') FILE_FORMAT = (TYPE = PARQUET);
```
2. Verify the loaded data:

```sql
SELECT * FROM books;

---
title                       |author             |date|
----------------------------+-------------------+----+
Transaction Processing      |Jim Gray           |1992|
Readings in Database Systems|Michael Stonebraker|2004|
```