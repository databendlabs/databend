---
title: isNull
---

Checks whether a value is NULL.

## Syntax

```sql
isNull(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | A value with non-compound data type. |

## Return Type

If x is NULL, ISNULL() returns 1, otherwise it returns 0.

## Examples

```sql
mysql> CREATE TABLE nullable_test (a UInt32, b UInt32) engine=Memory;
Query OK, 0 rows affected (3.19 sec)

mysql> INSERT INTO nullable_test VALUES(1, Null), (Null, 2), (3, 3);
Query OK, 0 rows affected (0.02 sec)

mysql> SELECT a FROM nullable_test WHERE isNull(b);
+------+
| a    |
+------+
|    1 |
+------+
1 row in set (0.17 sec)
```
