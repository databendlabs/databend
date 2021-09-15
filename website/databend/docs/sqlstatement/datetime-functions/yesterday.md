---
id: datetime-yesterday
title: YESTERDAY
---

Returns yesterday date, same as `today() - 1`.

## Syntax

```sql
YESTERDAY()
```

## Return Type

Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```
mysql> select YESTERDAY();
+-------------+
| YESTERDAY() |
+-------------+
| 2021-09-02  |
+-------------+

mysql> select TODAY()-1;
+---------------+
| (TODAY() - 1) |
+---------------+
| 2021-09-02    |
+---------------+
```
