---
title: DROP FUNCTION
---

Drop a function.

## Syntax

```sql
DROP FUNCTION [IF EXISTS] <function_name>
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

mysql> DROP FUNCTION IF EXISTS isnotempty;

mysql> SHOW FUNCTIONS LIKE 'isnotempty';
```
