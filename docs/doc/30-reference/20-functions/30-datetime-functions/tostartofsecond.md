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
SELECT toStartOfSecond(now());;
+------------------------+
| toStartOfSecond(now()) |
+------------------------+
| 2022-04-13 13:53:47    |
+------------------------+
```
