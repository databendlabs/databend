---
title: INSERT
---

Writing data.

## Insert Into Statement
### Syntax

```sql
INSERT INTO|OVERWRITE [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```


:::tip
Local engine is one of `Memory`, `Parquet`, `JSONEachRow`, `Null` or `CSV`, data will be stored in the DatabendQuery memory/fs locally.

Remote engine is `remote`, will be stored in the remote DatabendStore cluster.
:::

### Examples

#### Memory engine

Example:
```sql
CREATE TABLE test(a INT UNSIGNED, b Varchar) Engine = Fuse;

INSERT INTO test(a,b) values(888, 'stars');
INSERT INTO test values(1024, 'stars');

SELECT * FROM test;
+------+-------+
| a    | b     |
+------+-------+
|  888 | stars |
| 1024 | stars |
+------+-------+

INSERT OVERWRITE test values(2048, 'stars');
SELECT * FROM test;
+------+-------+
| a    | b     |
+------+-------+
| 2048 | stars |
+------+-------+
```

## Inserting the Results of SELECT
### Syntax

```
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

:::tip
Columns are mapped according to their position in the SELECT clause, So the number of columns in SELECT should be greater or equal to the INSERT table.

The data type of columns in the SELECT and INSERT table could be different, if necessary, type casting will be performed. 
:::

### Examples

Example:
```sql
CREATE TABLE select_table(a VARCHAR, b VARCHAR, c VARCHAR);
INSERT INTO select_table values('1','11','abc');

SELECT * FROM select_table;
+------+------+------+
| a    | b    | c    |
+------+------+------+
| 1    | 11   | abc  |
+------+------+------+

CREATE TABLE test(c1 TINTINT UNSIGNED, c2 BIGINT UNSIGNED, c3 VARCHAR) ;
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

## Insert with `DEFAULT` to fill default value

### Syntax

```sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v1|DEFAULT, v2|DEFAULT, v3|DEFAULT) ...
```

### Examples

```sql
create table t_insert_default(a int null, b int default 2, c float, d varchar default 'd');

insert into t_insert_default values (default, default, default, default), (1, default, 1.0, default), (3, 3, 3.0, default), (4, 4, 4.0, 'a');

select * from t_insert_default;
+------+------+------+------+
| a    | b    | c    | d    |
+------+------+------+------+
| NULL |    2 |  0.0 | d    |
|    1 |    2 |  1.0 | d    |
|    3 |    3 |  3.0 | d    |
|    4 |    4 |  4.0 | a    |
+------+------+------+------+
```