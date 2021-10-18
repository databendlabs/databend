---
id: datetime-tomorrow
title: TOMORROW
---

Returns tomorrow date, same as `today() + 1`.

## Syntax

```sql
YESTERDAY()
```

## Return Type

Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```
mysql> select TOMORROW();
+------------+
| TOMORROW() |
+------------+
| 2021-09-04 |
+------------+

mysql> select TODAY()+1;
+---------------+
| (TODAY() + 1) |
+---------------+
| 2021-09-04    |
+---------------+
```
