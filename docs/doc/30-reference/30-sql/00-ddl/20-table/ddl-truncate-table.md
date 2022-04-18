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
CREATE TABLE test(a UInt64, b Varchar) Engine = Memory;

INSERT INTO test(a,b) values(888, 'stars');

SELECT * FROM test;
+------+---------+
| a    | b       |
+------+---------+
|  888 |  stars  |
+------+---------+

TRUNCATE TABLE test;

SELECT * FROM test;

```
