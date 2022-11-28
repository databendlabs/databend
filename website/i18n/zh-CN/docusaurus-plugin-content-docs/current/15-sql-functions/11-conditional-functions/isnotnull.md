---
title: IS_NOT_NULL
description: 'is_not_null( <expr> ) function'
---

Checks whether a value is not NULL.

## Syntax

```sql
IS_NOT_NULL( <expr> )
```

## Arguments

| Arguments      | Description                                                  |
| -------------- | ------------------------------------------------------------ |
| `<expr>` | Any general expression which will be evaluated as the value. |

## Return Type

If x is not NULL, is_not_null() returns 1, otherwise it returns 0.

## Examples

```sql
CREATE TABLE nullable_test (a INT NULL, b INT UNSIGNED NULL);

INSERT INTO nullable_test VALUES(1, NULL), (NULL, 2), (3, 3);

SELECT a FROM nullable_test WHERE is_not_null(a);
+------+
| a    |
+------+
|    1 |
|    3 |
+------+
```
