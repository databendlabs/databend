---
title: DENSE_RANK
---


## DENSE_RANK

Returns the rank of a value in a group of values. This is similar to [RANK()](window-rank.md), except that tie values do not produce gaps in the sequence.

```sql
DENSE_RANK() OVER { named_window | inline_window }
```

## Examples

```sql
CREATE TABLE t1(a INT);

INSERT INTO t1 VALUES (1),(1),(1),(3),(3),(5),(5);

SELECT a, DENSE_RANK() OVER (ORDER BY a) rank FROM t1;
+------+------+
| a    | rank |
+------+------+
|    1 |    1 |
|    1 |    1 |
|    1 |    1 |
|    3 |    2 |
|    3 |    2 |
|    5 |    3 |
|    5 |    3 |
+------+------+
```
