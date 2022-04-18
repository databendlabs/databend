---
title: IS_NULL
description: 'IS_NULL( <expr> ) function'
---

Checks whether a value is NULL.

## Syntax

```sql
IS_NULL( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | Any general expression which will be evaluated as the value.

## Return Type

If x is NULL, IS_NULL() returns 1, otherwise it returns 0.

## Examples

```sql
mysql> CREATE TABLE nullable_test (a INT NULL, b INT UNSIGNED NULL);

mysql> INSERT INTO nullable_test VALUES(1, NULL), (NULL, 2), (3, 3);

mysql> SELECT a FROM nullable_test WHERE is_null(b);
+------+
| a    |
+------+
|    1 |
+------+
```
