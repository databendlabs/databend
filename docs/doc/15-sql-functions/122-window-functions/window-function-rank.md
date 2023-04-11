---
title: RANK
---


## RANK

Returns the rank of a value within an ordered group of values.

The rank value starts at 1 and continues up sequentially.

If two values are the same, they have the same rank.

## Syntax

```sql
RANK() OVER ( [ PARTITION BY <expr1> ] ORDER BY <expr2> [ { ASC | DESC } ] [ <window_frame> ] )
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
