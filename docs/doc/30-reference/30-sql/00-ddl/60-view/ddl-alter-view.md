---
title: ALTER VIEW
description:
  Modifies the properties for an existing view.
---

Alter the existing view by using another `QUERY`.

## Syntax

```sql
ALTER VIEW [db.]view_name AS SELECT query
```

## Examples

```sql
create view tmp_view as SELECT number % 3 as a, avg(number) from numbers(1000) group by a order by a;

SELECT * from tmp_view;
+------+-------------+
| a    | avg(number) |
+------+-------------+
|    0 |       499.5 |
|    1 |         499 |
|    2 |         500 |
+------+-------------+

ALTER VIEW tmp_view AS SELECT * from numbers(3);

SELECT * from tmp_view;
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
+--------+
```
