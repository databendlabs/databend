---
title: isNotNull
---

Checks whether a value is not NULL.

## Syntax

```sql
isNotNull(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | A value with non-compound data type. |

## Return Type

If x is not NULL, isNotNull() returns 1, otherwise it returns 0.

## Examples

```sql
mysql> CREATE TABLE nullable_test (a UInt32, b UInt32) engine=Memory;
Query OK, 0 rows affected (3.19 sec)

mysql> INSERT INTO nullable_test VALUES(1, Null), (Null, 2), (3, 3);
Query OK, 0 rows affected (0.02 sec)

mysql> SELECT a FROM nullable_test WHERE isNotNull(a);
+------+
| a    |
+------+
|    1 |
|    3 |
+------+
2 rows in set (0.01 sec)
```
