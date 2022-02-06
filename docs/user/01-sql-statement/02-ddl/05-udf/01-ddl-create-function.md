---
title: CREATE FUNCTION
---

Create a new function.

## Syntax

```sql
CREATE FUNCTION [IF NOT EXISTS] <function_name> AS (<params>) -> <definition> [DESC = '<string>']
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
```
