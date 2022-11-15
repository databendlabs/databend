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
CREATE TABLE test(a BIGINT UNSIGNED, b VARCHAR) Engine = Fuse;

INSERT INTO test(a,b) VALUES(888, 'stars');

SELECT * FROM test;
+------+---------+
| a    | b       |
+------+---------+
|  888 |  stars  |
+------+---------+

TRUNCATE TABLE test;

SELECT * FROM test;
```
