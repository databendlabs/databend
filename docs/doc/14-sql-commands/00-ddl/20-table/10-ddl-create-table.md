---
title: CREATE TABLE
description: Create a new table.
---

Creating tables is one of the most complicated operations for many databases because you might need to:

* Manually specify the engine
* Manually specify the indexes
* And even specify the data partitions or data shard

Databend aims to be easy to use by design and does NOT require any of those operations when you create a table. Moreover, the CREATE TABLE statement provides these options to make it much easier for you to create tables in various scenarios:

- [CREATE TABLE](#create-table): Creates a table from scratch.
- [CREATE TABLE ... LIKE](#create-table--like): Creates a table with the same column definitions as an existing one.
- [CREATE TABLE ... AS](#create-table--as): Creates a table and inserts data with the results of a SELECT query.
- [CREATE TRANSIENT TABLE](#create-transient-table): Creates a table without storing its historical data for Time Travel.
- [CREATE TABLE ... SNAPSHOT_LOCATION](#create-table--snapshot_location): Creates a table and inserts data with a snapshot file.
- [CREATE TABLE ... EXTERNAL_LOCATION](#create-table--external_location): Creates a table and specifies an S3 bucket for the data storage instead of the FUSE engine.

## CREATE TABLE

```sql
CREATE [TRANSIENT] TABLE [IF NOT EXISTS] [db.]table_name
(
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    ...
)
```
:::note
- For available data types in Databend, see [Data Types](../../../13-sql-reference/10-data-types/index.md).

- Databend suggests avoiding special characters as much as possible when naming columns. However, if special characters are necessary in some cases, the alias should be enclosed in backticks, like this: CREATE TABLE price(\`$CA\` int);

- Databend will automatically convert column names into lowercase. For example, if you name a column as *Total*, it will appear as *total* in the result.
:::


## CREATE TABLE ... LIKE

Creates a table with the same column definitions as an existing table. Column names, data types, and their non-NUll constraints of the existing will be copied to the new table.

Syntax:
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
LIKE [db.]origin_table_name
```

This command does not include any data or attributes (such as `CLUSTER BY`, `TRANSIENT`, and `COMPRESSION`) from the original table, and instead creates a new table using the default system settings.

:::note WORKAROUND
- `TRANSIENT` and `COMPRESSION` can be explicitly specified when you create a new table with this command. For example,

```sql
create transient table t_new like t_old;

create table t_new compression='lz4' like t_old;
```
:::

## CREATE TABLE ... AS

Creates a table and fills it with data computed by a SELECT command.

Syntax:
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
AS SELECT query
```

This command does not include any attributes (such as CLUSTER BY, TRANSIENT, and COMPRESSION) from the original table, and instead creates a new table using the default system settings.

:::note WORKAROUND
- `TRANSIENT` and `COMPRESSION` can be explicitly specified when you create a new table with this command. For example,

```sql
create transient table t_new as select * from t_old;

create table t_new compression='lz4' as select * from t_old;
```
:::

## CREATE TRANSIENT TABLE

Creates a transient table. 

Transient tables are used to hold transitory data that does not require a data protection or recovery mechanism. Dataebend does not hold historical data for a transient table so you will not be able to query from a previous version of the transient table with the Time Travel feature, for example, the [AT](./../../20-query-syntax/03-query-at.md) clause in the SELECT statement will not work for transient tables. Please note that you can still [drop](./20-ddl-drop-table.md) and [undrop](./21-ddl-undrop-table.md) a transient table.

Transient tables help save your storage expenses because they do not need extra space for historical data compared to non-transient tables. See [example](#create-transient-table-1) for detailed explanations.

Syntax:
```sql
CREATE TRANSIENT TABLE ...
```

## CREATE TABLE ... SNAPSHOT_LOCATION

Creates a table and inserts data from a snapshot file. 

Databend automatically creates snapshots when data updates occur, so a snapshot can be considered as a view of your data at a time point in the past. Databend may store many snapshots of a table (depending on the number of update operations you performed) for the Time Travel feature that allows you to query, back up, and restore from a previous version of your data within the retention period (24 hours by default).

This command enables you to insert the data stored in a snapshot file when you create a table. Please note that the table you create must have same column definitions as the data from the snapshot.

Syntax:
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    ...
)
SNAPSHOT_LOCATION = '<SNAPSHOT_FILENAME>';
```

To obtain the snapshot information (including the snapshot locations) of a table, execute the following command:

```sql
SELECT * 
FROM   Fuse_snapshot('<database_name>', '<table_name>'); 
```

## CREATE TABLE ... EXTERNAL_LOCATION

Creates a table and specifies an S3 bucket for the data storage instead of the FUSE engine.

Databend stores the table data in the location configured in the file `databend-query.toml` by default. This option enables you to store the data (in parquet format) in a table in another bucket instead of the default one.

Syntax:
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name

    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    ...

's3://<bucket>/[<path>]' 
CONNECTION = (
        ENDPOINT_URL = 'https://<endpoint-URL>'
        ACCESS_KEY_ID = '<your-access-key-ID>'
        SECRET_ACCESS_KEY = '<your-secret-access-key>'
        REGION = '<region-name>'
        ENABLE_VIRTUAL_HOST_STYLE = 'true'|'false'
  );
```

| Parameter                   | Description                                                                                                                                                                                                              | Required   |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|
| `s3://<bucket>/[<path>]`    | Files are in the specified external location (S3-like bucket)                                                                                                                                                            | YES        |
| ENDPOINT_URL              	 | The bucket endpoint URL starting with "https://". To use a URL starting with "http://", set `allow_insecure` to `true` in the [storage] block of the file `databend-query-node.toml`.                                  	 | Optional 	 |
| ACCESS_KEY_ID             	 | Your access key ID for connecting the AWS S3 compatible object storage. If not provided, Databend will access the bucket anonymously.    	                                                                               | Optional 	 |
| SECRET_ACCESS_KEY         	 | Your secret access key for connecting the AWS S3 compatible object storage. 	                                                                                                                                            | Optional 	 |
| REGION                    	 | AWS region name. For example, us-east-1.                                    	                                                                                                                                            | Optional 	 |
| ENABLE_VIRTUAL_HOST_STYLE 	 | If you use virtual hosting to address the bucket, set it to "true".                               	                                                                                                                      | Optional 	 |

## Column Nullable

By default, **all columns are not nullable(NOT NULL)**, if you want to specify a column default to `NULL`, please use:
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    <column_name> <data_type> NULL,
     ...
)
```

Let check it out how difference the column is `NULL` or `NOT NULL`.

Create a table `t_not_null` which column with `NOT NULL`(Databend Column is `NOT NULL` by default):
```sql
CREATE TABLE t_not_null(a INT);
```

```sql
DESC t_not_null;
+-------+-------+------+---------+
| Field | Type  | Null | Default |
+-------+-------+------+---------+
| a     | Int32 | NO   | 0       |
+-------+-------+------+---------+
```

Create another table `t_null` column with `NULL`:
```sql
CREATE TABLE t_null(a INT NULL);
```

```sql
DESC t_null;
+-------+-------+------+---------+
| Field | Type  | Null | Default |
+-------+-------+------+---------+
| a     | Int32 | YES  | NULL    |
+-------+-------+------+---------+
```

## Default Values
```sql
DEFAULT <expr>
```
Specify a default value inserted in the column if a value is not specified via an `INSERT` or `CREATE TABLE AS SELECT` statement.

For example:
```sql
CREATE TABLE t_default_value(a TINYINT UNSIGNED, b VARCHAR DEFAULT 'b');
```

Desc the `t_default_value` table:
```sql
DESC t_default_value;
+-------+------------------+------+---------+-------+
| Field | Type             | Null | Default | Extra |
+-------+------------------+------+---------+-------+
| a     | TINYINT UNSIGNED | NO   | 0       |       |
| b     | VARCHAR          | NO   | b       |       |
+-------+------------------+------+---------+-------+
```

Insert a value:
```sql
INSERT INTO T_default_value(a) VALUES(1);
```

Check the table values:
```sql
SELECT * FROM t_default_value;
+------+------+
| a    | b    |
+------+------+
|    1 | b    |
+------+------+
```

## MySQL Compatibility

Databend’s syntax is difference from MySQL mainly in the data type and some specific index hints.

## Examples

### Create Table

Create a table with a default value for a column (in this case, the `genre` column has 'General' as the default value):

```sql
CREATE TABLE books (
    id BIGINT UNSIGNED,
    title VARCHAR,
    genre VARCHAR DEFAULT 'General'
);
```

Describe the table to confirm the structure and the default value for the `genre` column:

```sql
DESC books;
+-------+-----------------+------+---------+-------+
| Field | Type            | Null | Default | Extra |
+-------+-----------------+------+---------+-------+
| id    | BIGINT UNSIGNED | NO   | 0       |       |
| title | VARCHAR         | NO   | ""      |       |
| genre | VARCHAR         | NO   | 'General'|       |
+-------+-----------------+------+---------+-------+
```

Insert a row without specifying the `genre`:

```sql
INSERT INTO books(id, title) VALUES(1, 'Invisible Stars');
```

Query the table and notice that the default value 'General' has been set for the `genre` column:

```sql
SELECT * FROM books;
+----+----------------+---------+
| id | title          | genre   |
+----+----------------+---------+
|  1 | Invisible Stars| General |
+----+----------------+---------+
```

### Create Table ... Like

Create a new table (`books_copy`) with the same structure as an existing table (`books`):

```sql
CREATE TABLE books_copy LIKE books;
```

Check the structure of the new table:

```sql
DESC books_copy;
+-------+-----------------+------+---------+-------+
| Field | Type            | Null | Default | Extra |
+-------+-----------------+------+---------+-------+
| id    | BIGINT UNSIGNED | NO   | 0       |       |
| title | VARCHAR         | NO   | ""      |       |
| genre | VARCHAR         | NO   | 'General'|       |
+-------+-----------------+------+---------+-------+
```

Insert a row into the new table and notice that the default value for the `genre` column has been copied:

```sql
INSERT INTO books_copy(id, title) VALUES(1, 'Invisible Stars');

SELECT * FROM books_copy;
+----+----------------+---------+
| id | title          | genre   |
+----+----------------+---------+
|  1 | Invisible Stars| General |
+----+----------------+---------+
```

### Create Table ... As

Create a new table (`books_backup`) that includes data from an existing table (`books`):

```sql
CREATE TABLE books_backup AS SELECT * FROM books;
```

Describe the new table and notice that the default value for the `genre` column has NOT been copied:

```sql
DESC books_backup;
+-------+-----------------+------+---------+-------+
| Field | Type            | Null | Default | Extra |
+-------+-----------------+------+---------+-------+
| id    | BIGINT UNSIGNED | NO   | 0       |       |
| title | VARCHAR         | NO   | ""      |       |
| genre | VARCHAR         | NO   | NULL    |       |
+-------+-----------------+------+---------+-------+
```

Query the new table and notice that the data from the original table has been copied:

```sql
SELECT * FROM books_backup;
+----+----------------+---------+
| id | title          | genre   |
+----+----------------+---------+
|  1 | Invisible Stars| General |
+----+----------------+---------+
```

### Create Transient Table

Create a transient table (temporary table) that automatically deletes data after a specified period of time:

```sql
-- Create a transient table
CREATE TRANSIENT TABLE visits (
  visitor_id BIGINT
);

-- Insert values
INSERT INTO visits VALUES(1);
INSERT INTO visits VALUES(2);
INSERT INTO visits VALUES(3);

-- Check the inserted data
SELECT * FROM visits;
+-----------+
| visitor_id |
+-----------+
|         1 |
|         2 |
|         3 |
+-----------+
```

### Create Table ... Snapshot_Location

Create a table from a specific snapshot, allowing you to create a new table based on the data at a specific point in time:

```sql
CREATE TABLE members 
  ( 
     name VARCHAR 
  ); 

INSERT INTO members 
VALUES     ('Amy'); 

SELECT snapshot_id, 
       timestamp, 
       snapshot_location 
FROM   fuse_snapshot('default', 'members'); 
+-----------------------------------+----------------------------+------------------------------------------------------------+
| snapshot_id                       | timestamp                  | snapshot_location                                          |
+-----------------------------------+----------------------------+------------------------------------------------------------+
|  b5931727ee404869ab99b25bf9e672a9 | 2022-08-29 17:53:54.243561 | 418920/604411/_ss/b5931727ee404869ab99b25bf9e672a9_v1.json |
+-----------------------------------+----------------------------+------------------------------------------------------------+

INSERT INTO members 
VALUES     ('Bob'); 

SELECT snapshot_id, 
       timestamp, 
       snapshot_location 
FROM   fuse_snapshot('default', 'members'); 
+----------------------------------+----------------------------+------------------------------------------------------------+
| snapshot_id                      | timestamp                  | snapshot_location                                          |
|----------------------------------|----------------------------|------------------------------------------------------------|
| b5931727ee404869ab99b25bf9e672a9 | 2022-08-29 17:53:54.243561 | 418920/604411/_ss/b5931727ee404869ab99b25bf9e672a9_v1.json |
| 12637e70dd1c4abbab15470fa0a6d69b | 2022-08-29 18:04:18.973272 | 418920/604411/_ss/12637e70dd1c4abbab15470fa0a6d69b_v1.json |
+----------------------------------+----------------------------+------------------------------------------------------------+

-- Create a new table with a snapshot (ID: b5931727ee404869ab99b25bf9e672a9)
CREATE TABLE members_previous 
  ( 
     name VARCHAR 
  )
snapshot_location='418920/604411/_ss/b5931727ee404869ab99b25bf9e672a9_v1.json';

-- The created table contains "Amy" that is stored in the snapshot
SELECT * 
FROM   members_previous; 
---
Amy
```

### Create Table ... External_Location

Create a table with data stored on an external location, such as Amazon S3:

```sql
-- Create a table named `mytable` and specify the location `s3://testbucket/admin/data/` for the data storage
CREATE TABLE mytable (
  a INT
) 
's3://testbucket/admin/data/' 
CONNECTION=(
  ACCESS_KEY_ID='<your_aws_key_id>' 
  SECRET_ACCESS_KEY='<your_aws_secret_key>' 
  ENDPOINT_URL='https://s3.amazonaws.com'
);
```
