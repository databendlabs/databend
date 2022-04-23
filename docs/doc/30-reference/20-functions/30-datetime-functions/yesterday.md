---
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

```sql
SELECT YESTERDAY();
+-------------+
| YESTERDAY() |
+-------------+
| 2021-09-02  |
+-------------+

SELECT TODAY()-1;
+---------------+
| (TODAY() - 1) |
+---------------+
| 2021-09-02    |
+---------------+
```
