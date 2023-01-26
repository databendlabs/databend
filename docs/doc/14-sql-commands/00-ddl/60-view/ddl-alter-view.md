---
title: ALTER VIEW
description:
  Modifies the properties for an existing view.
---

Alter the existing view by using another `QUERY`.

## Syntax

```sql
ALTER VIEW [db.]view_name [(<column>, ...)] AS SELECT query
```

## Examples

```sql
CREATE VIEW tmp_view AS SELECT number % 3 AS a, avg(number) FROM numbers(1000) GROUP BY a ORDER BY a;

SELECT * FROM tmp_view;
+------+-------------+
| a    | avg(number) |
+------+-------------+
|    0 |       499.5 |
|    1 |       499.0 |
|    2 |       500.0 |
+------+-------------+

ALTER VIEW tmp_view(c1) AS SELECT * from numbers(3);

SELECT * FROM tmp_view;
+------+
| c1   |
+------+
|    0 |
|    1 |
|    2 |
+------+
```
