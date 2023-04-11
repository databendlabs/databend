---
title: RANK
---


## RANK

Returns the rank of a value in a group of values. The rank is one plus the number of rows preceding the row that are not peer with the row. 
Thus, tie values in the ordering will produce gaps in the sequence. The ranking is performed for each window partition.

```sql
RANK() OVER { named_window | inline_window }
```

## Examples

```sql
CREATE TABLE t1(a INT);

INSERT INTO t1 VALUES (1),(1),(1),(3),(3),(5),(5);

SELECT a, RANK() OVER (ORDER BY a) rank FROM t1;
+------+------+
| a    | rank |
+------+------+
|    1 |    1 |
|    1 |    1 |
|    1 |    1 |
|    3 |    4 |
|    3 |    4 |
|    5 |    6 |
|    5 |    6 |
+------+------+
```
