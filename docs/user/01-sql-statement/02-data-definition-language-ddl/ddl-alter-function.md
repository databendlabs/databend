---
title: ALTER FUNCTION
---

Alter an existing function.

## Syntax

```sql
ALTER FUNCTION <function_name> AS (<params>) -> <definition> [DESC = '<string>']
```

## Examples

```sql
mysql> CREATE FUNCTION IF NOT EXISTS isnotempty AS (p) -> not(isnull(p)) DESC = 'This is a description';

mysql> SHOW FUNCTIONS LIKE 'isnotempty';
+------------+------------+--------------+----------------+-----------------------+
| name       | is_builtin | is_aggregate | definition     | description           |
+------------+------------+--------------+----------------+-----------------------+
| isnotempty |          0 |            0 | not(isnull(p)) | This is a description |
+------------+------------+--------------+----------------+-----------------------+

mysql> ALTER FUNCTION isnotempty AS (p) -> isnotnull(p) DESC = 'This is a new description';

mysql> SHOW FUNCTIONS LIKE 'isnotempty';
+------------+------------+--------------+--------------+---------------------------+
| name       | is_builtin | is_aggregate | definition   | description               |
+------------+------------+--------------+--------------+---------------------------+
| isnotempty |          0 |            0 | isnotnull(p) | This is a new description |
+------------+------------+--------------+--------------+---------------------------+
```
