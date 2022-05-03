---
title: toStartOfSecond
---

Rounds down a date with time to the start of the second.

## Syntax

```sql
toStartOfSecond(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
mysql> select toStartOfSecond(now());;
+------------------------+
| toStartOfSecond(now()) |
+------------------------+
| 2022-04-13 13:53:47    |
+------------------------+

mysql> select toStartOfSecond(toDateTime(1649858064));
+-----------------------------------------+
| toStartOfSecond(toDateTime(1649858064)) |
+-----------------------------------------+
| 2022-04-13 13:54:24                     |
+-----------------------------------------+
```
