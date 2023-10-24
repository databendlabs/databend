---
title: INSERT
---

Writes data into a table.

:::tip atomic operations
Databend ensures data integrity with atomic operations. Inserts, updates, replaces, and deletes either succeed completely or fail entirely.
:::

## Insert Direct Values

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

## Insert Query Results

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

Databend allows you to use the INSERT INTO statement to add data into a table, specifying values or defaults for columns as needed.

### Syntax

```sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v1|DEFAULT, v2|DEFAULT, v3|DEFAULT) ...
```

### Examples

```sql
CREATE TABLE t_insert_default(a int null, b int default 2, c float, d varchar default 'd');

INSERT INTO t_insert_default
VALUES
    (default, default, default, default),
    (1, default, 1.0, default),
    (3, 3, 3.0, default),
    (4, 4, 4.0, 'a');

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

Databend enables you to insert data into a table from staged files with the INSERT INTO statement. This is achieved through Databend's capacity to [Query Staged Files](../../12-load-data/00-transform/05-querying-stage.md) and subsequently incorporate the query result into the table.

### Syntax

```sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

### Examples

1. Create a table called `sample`:

```sql
CREATE TABLE sample
(
    id      INT,
    city    VARCHAR,
    score   INT,
    country VARCHAR DEFAULT 'China'
);
```

2. Set up an internal stage with sample data

We'll establish an internal stage named `mystage` and then populate it with sample data.

```sql
CREATE STAGE mystage;
       
COPY INTO @mystage
FROM 
(
    SELECT * 
    FROM 
    (
        VALUES 
        (1, 'Chengdu', 80),
        (3, 'Chongqing', 90),
        (6, 'Hangzhou', 92),
        (9, 'Hong Kong', 88)
    )
)
FILE_FORMAT = (TYPE = PARQUET);
```

3. Insert data from the staged Parquet file with `INSERT INTO`

:::tip
You can specify the file format and various copy-related settings with the FILE_FORMAT and COPY_OPTIONS available in the [COPY INTO](dml-copy-into-table.md) command. When `purge` is set to `true`, the original file will only be deleted if the data update is successful. 
:::

```sql
INSERT INTO sample 
    (id, city, score) 
ON
    (Id)
SELECT
    $1, $2, $3
FROM
    @mystage
    (FILE_FORMAT => 'parquet');
```

4. Verify the data insert

```sql
SELECT * FROM sample;
```

The results should be:
```sql
┌─────────────────────────────────────────────────────────────────────────┐
│        id       │       city       │      score      │      country     │
│ Nullable(Int32) │ Nullable(String) │ Nullable(Int32) │ Nullable(String) │
├─────────────────┼──────────────────┼─────────────────┼──────────────────┤
│               1 │ Chengdu          │              80 │ China            │
│               3 │ Chongqing        │              90 │ China            │
│               6 │ Hangzhou         │              92 │ China            │
│               9 │ Hong Kong        │              88 │ China            │
└─────────────────────────────────────────────────────────────────────────┘
```