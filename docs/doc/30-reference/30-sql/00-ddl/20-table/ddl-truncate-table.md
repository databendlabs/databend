---
title: TRUNCATE TABLE
---

Empties the table completely.

## Syntax

```sql
TRUNCATE TABLE [db.]name
```

## Examples

```sql
mysql> CREATE TABLE test(a UInt64, b Varchar) Engine = Memory;

mysql> INSERT INTO test(a,b) values(888, 'stars');

mysql> SELECT * FROM test;
+------+---------+
| a    | b       |
+------+---------+
|  888 |  stars  |
+------+---------+

mysql> TRUNCATE TABLE test;

mysql> SELECT * FROM test;

```
