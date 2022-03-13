---
title: SHOW FUNCTIONS
---

Shows the list of supported functions currently, including builtin scalar/aggregate functions and user defined functions.

## Syntax

```
SHOW FUNCTIONS  [LIKE 'pattern' | WHERE expr]
```

## Example

```sql
mysql> SHOW FUNCTIONS;
+-------------------------+------------+--------------+-------------------+---------------------------+
| name                    | is_builtin | is_aggregate | definition        | description               |
+-------------------------+------------+--------------+-------------------+---------------------------+
| !=                      |          1 |            0 |                   |                           |
| %                       |          1 |            0 |                   |                           |
| *                       |          1 |            0 |                   |                           |
| +                       |          1 |            0 |                   |                           |
| -                       |          1 |            0 |                   |                           |
| /                       |          1 |            0 |                   |                           |
| <                       |          1 |            0 |                   |                           |
| <=                      |          1 |            0 |                   |                           |
| <>                      |          1 |            0 |                   |                           |
| =                       |          1 |            0 |                   |                           |
+-------------------------+------------+--------------+-------------------+---------------------------+
```

Showing the functions begin with `"today"`:
```sql
mysql> SHOW FUNCTIONS LIKE 'today%';
+--------------+------------+--------------+------------+-------------+
| name         | is_builtin | is_aggregate | definition | description |
+--------------+------------+--------------+------------+-------------+
| today        |          1 |            0 |            |             |
| todayofmonth |          1 |            0 |            |             |
| todayofweek  |          1 |            0 |            |             |
| todayofyear  |          1 |            0 |            |             |
+--------------+------------+--------------+------------+-------------+
```

Showing the functions begin with `"today"` with `WHERE`:
```sql
mysql> SHOW FUNCTIONS WHERE name LIKE 'today%';
+--------------+------------+--------------+------------+-------------+
| name         | is_builtin | is_aggregate | definition | description |
+--------------+------------+--------------+------------+-------------+
| today        |          1 |            0 |            |             |
| todayofmonth |          1 |            0 |            |             |
| todayofweek  |          1 |            0 |            |             |
| todayofyear  |          1 |            0 |            |             |
+--------------+------------+--------------+------------+-------------+
```
