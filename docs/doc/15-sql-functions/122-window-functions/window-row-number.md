---
title: ROW_NUMBER
---


## ROW_NUMBER

Returns a unique, sequential number for each row, starting with one, according to the ordering of rows within the window partition.

```sql
ROW_NUMBER() OVER { named_window | inline_window }
```

## Examples

```sql
CREATE TABLE t1(a INT);

INSERT INTO t1 VALUES (1),(1),(1),(3),(3),(5),(5);

SELECT a, ROW_NUMBER() OVER (PARTITION BY a) FROM t1;
+------+------------------------------------+
| a    | row_number() over (partition by a) |
+------+------------------------------------+
|    1 |                                  1 |
|    1 |                                  2 |
|    1 |                                  3 |
|    3 |                                  1 |
|    3 |                                  2 |
|    5 |                                  1 |
|    5 |                                  2 |
+------+------------------------------------+
```
