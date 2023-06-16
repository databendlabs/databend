---
title: INSERT INTO
---

Writes data into a table.

:::tip atomic operations
Databend ensures data integrity with atomic operations. Inserts, updates, replaces, and deletes either succeed completely or fail entirely.
:::

## Insert Values

### Syntax

```sql
INSERT INTO|OVERWRITE [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

### Examples

```sql
CREATE TABLE test(a INT UNSIGNED, b Varchar);

INSERT INTO test(a,b) VALUES(888, 'stars');
INSERT INTO test VALUES(1024, 'stars');

SELECT * FROM test;
+------+-------+
| a    | b     |
+------+-------+
|  888 | stars |
| 1024 | stars |
+------+-------+

INSERT OVERWRITE test VALUES(2048, 'stars');
SELECT * FROM test;
+------+-------+
| a    | b     |
+------+-------+
| 2048 | stars |
+------+-------+
```

## Insert Results of SELECT

When inserting the results of a SELECT statement, the mapping of columns follows their positions in the SELECT clause. Therefore, the number of columns in the SELECT statement must be equal to or greater than the number of columns in the INSERT table. In cases where the data types of the columns in the SELECT statement and the INSERT table differ, type casting will be performed as needed.

### Syntax

```sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

### Examples

```sql
CREATE TABLE select_table(a VARCHAR, b VARCHAR, c VARCHAR);
INSERT INTO select_table VALUES('1','11','abc');

SELECT * FROM select_table;
+------+------+------+
| a    | b    | c    |
+------+------+------+
| 1    | 11   | abc  |
+------+------+------+

CREATE TABLE test(c1 TINTINT UNSIGNED, c2 BIGINT UNSIGNED, c3 VARCHAR);
INSERT INTO test SELECT * FROM select_table;

SELECT * from test;
+------+------+------+
| c1   | c2   | c3   |
+------+------+------+
|    1 |   11 | abc  |
+------+------+------+
```

Aggregate Example:

```sql
-- create table
CREATE TABLE base_table(a INT);
CREATE TABLE aggregate_table(b INT);

-- insert some data to base_table
INSERT INTO base_table VALUES(1),(2),(3),(4),(5),(6);

-- insert into aggregate_table from the aggregation
INSERT INTO aggregate_table SELECT SUM(a) FROM base_table GROUP BY a%3;

SELECT * FROM aggregate_table ORDER BY b;
+------+
| b    |
+------+
|    5 |
|    7 |
|    9 |
+------+
```

## Insert Default Values

### Syntax

```sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v1|DEFAULT, v2|DEFAULT, v3|DEFAULT) ...
```

### Examples

```sql
CREATE TABLE t_insert_default(a int null, b int default 2, c float, d varchar default 'd');

INSERT INTO t_insert_default VALUES (default, default, default, default), (1, default, 1.0, default), (3, 3, 3.0, default), (4, 4, 4.0, 'a');

SELECT * FROM t_insert_default;
+------+------+------+------+
| a    | b    | c    | d    |
+------+------+------+------+
| NULL |    2 |  0.0 | d    |
|    1 |    2 |  1.0 | d    |
|    3 |    3 |  3.0 | d    |
|    4 |    4 |  4.0 | a    |
+------+------+------+------+
```

## Insert with Staged Files

Databend allows you to insert data from a staged file into a table by utilizing the INSERT INTO statement with its [HTTP Handler](../../11-integrations/00-api/00-rest.md).

### Syntax

```sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES ...
```

### Examples

This example showcases the usage of Databend's [HTTP handler](../../11-integrations/00-api/01-mysql-handler.md) to insert data from a staged CSV file into a table. 

```sql
CREATE TABLE t_insert_stage(a int null, b int default 2, c float, d varchar default 'd');
```

Upload `values.csv` to a stage:

```plain title='values.csv'
1,1.0
2,2.0
3,3.0
4,4.0
```

```shell title='Request /v1/upload_to_stage' API
curl -H "stage_name:my_int_stage" -F "upload=@./values.csv" -XPUT http://root:@localhost:8000/v1/upload_to_stage
```

Insert with the uploaded file:

```shell
curl -d '{"sql": "insert into t_insert_stage (a, c) values", "stage_attachment": {"location": "@my_int_stage/values.csv", "file_format_options": {}, "copy_options": {}}}' -H 'Content-type: application/json' http://root:@localhost:8000/v1/query
```

:::tip
You can specify the file format and various copy-related settings with the FILE_FORMAT and COPY_OPTIONS available in the [COPY INTO](dml-copy-into-table.md) command. When `purge` is set to `true`, the original file will only be deleted if the data update is successful. 
:::

Verify the inserted data:

```sql
SELECT * FROM t_insert_stage;
+------+------+------+------+
| a    | b    | c    | d    |
+------+------+------+------+
|    1 |    2 |  1.0 | d    |
|    2 |    2 |  2.0 | d    |
|    3 |    2 |  3.0 | d    |
|    4 |    2 |  4.0 | d    |
+------+------+------+------+
```