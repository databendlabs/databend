---
title: CREATE TABLE
---

Create a new table.

## Syntax

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    <col_name> <col_type> [ { DEFAULT <expr> }] [ NOT NULL | NULL],
    <col_name> <col_type> [ { DEFAULT <expr> }] [ NOT NULL | NULL],
    ...
)
```

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
LIKE [db.]origin_table_name
```
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
LIKE [db.]origin_table_name
AS SELECT query
```

## Column Option is nullable or not

By default, **all columns are not nullable(NOT NULL)**, if you want to special a column default to `NULL`, please use:
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    <col_name> <col_type> NULL,
     ...
)
```

```sql
create table t1(a int NULL);
```

## Default Values
```sql
DEFAULT <expression>
```
Specifies a default value inserted in the column if a value is not specified via an INSERT or CREATE TABLE AS SELECT statement.


## Examples

```sql
mysql> CREATE TABLE test(a UInt64, b Varchar, c Varchar DEFAULT concat(b, '-b'));

mysql> INSERT INTO test(a,b) values(888, 'stars');

mysql> select * from test;
+------+-------+---------+
| a    | b     | c       |
+------+-------+---------+
|  888 | stars | stars-b |
+------+-------+---------+
```
### Create Table Like statement
```sql
mysql> CREATE TABLE test(a UInt64, b Varchar);

mysql> INSERT INTO test(a,b) values(888, 'stars');

mysql> SELECT * FROM test;
+------+---------+
| a    | b       |
+------+---------+
|  888 |  stars  |
+------+---------+

mysql> CREATE TABLE test2 LIKE test;

mysql> INSERT INTO test2(a,b) values(0, 'sun');

mysql> SELECT * FROM test2;
+------+------+
| a    | b    |
+------+------+
|    0 | sun  |
+------+------+
```

### Create Table As Select (CTAS) statement

```sql
mysql> CREATE TABLE source(a UInt64 null, b Varchar null)

mysql> INSERT INTO source(a,b) values(888, 'stars');

mysql> SELECT * FROM source;
+------+---------+
| a    | b       |
+------+---------+
|  888 |  stars  |
+------+---------+

mysql> CREATE TABLE copy1 AS SELECT * FROM source;

mysql> SELECT * FROM copy1;
+------+-------+
| a    | b     |
+------+-------+
|  888 | stars |
+------+-------+

mysql> CREATE TABLE copy2(x VARCHAR NULL, y VARCHAR NULL) AS SELECT * FROM source;

mysql> SELECT * FROM copy2;
+------+------+------+-------+
| x    | y    | a    | b     |
+------+------+------+-------+
| NULL | NULL |  888 | stars |
+------+------+------+-------+
```
