---
title: IS [NOT] DISTINCT_FROM
description: '<expr1> IS [NOT] DISTINCT_FROM <expr2> function'
---

Compares whether two expressions are equal (or not equal) with awareness of nullability, meaning it treats NULLs as known values for comparing equality. Note that this is different from the [comparision operators](../02-comparisons-operators/), which will return NULL if the arguments are NULL.

`IS [NOT] DISTINCT FROM ` is available under planner_v2.

## Syntax

```sql
<expr1> IS [ NOT ] DISTINCT FROM <expr2>
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr1>` | Any general expression which will be evaluated as the value.
| `<expr2>` | Any general expression which will be evaluated as the value.

## Return Type

For `IS DISTINCT FROM ` operator, if x and y are NULL, it returns 0; and if only one of x and y is NULL, it returns 1; otherwise it returns the result of `x<>y`, and vice versa for `IS NOT DISTINCT FROM`.

## Examples

```sql
CREATE TABLE t_null (a INT NULL, b INT UNSIGNED NULL);

INSERT INTO t_null VALUES(1, NULL), (NULL, 2), (NULL, NULL),  (1, 2), (3, 3);

SET enable_planner_v2 = 1;

SELECT a, b FROM t_null WHERE a IS DISTINCT FROM b;
+------+------+
| a    | b    |
+------+------+
|    1 | NULL |
| NULL |    2 |
|    1 |    2 |
+------+------+

SELECT a, b FROM t_null WHERE a IS NOT DISTINCT FROM b;
+------+------+
| a    | b    |
+------+------+
| NULL | NULL |
|    3 |    3 |
+------+------+
```
