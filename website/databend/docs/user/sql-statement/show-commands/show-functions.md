---
title: SHOW FUNCTIONS
---

Shows the list of supported functions currently.

## Syntax

```
SHOW FUNCTIONS  [LIKE 'pattern' | WHERE expr]
```

## Example

```sql
mysql> SHOW FUNCTIONS;
+-------------------------+
| name                    |
+-------------------------+
| !=                      |
| %                       |
| *                       |
| +                       |
| -                       |
| /                       |
| <                       |
| <=                      |
| <>                      |
| =                       |
| >                       |
| >=                      |
| abs                     |
| acos                    |
| adddays                 |
| addhours                |
+-------------------------+
```

Showing the functions begin with `"today"`:
```sql
mysql> SHOW FUNCTIONS LIKE 'today%';
+--------------+
| name         |
+--------------+
| today        |
| todayofmonth |
| todayofweek  |
| todayofyear  |
+--------------+
```

Showing the functions begin with `"today"` with `WHERE`:
```sql
mysql> SHOW FUNCTIONS WHERE name LIKE 'today%';
+--------------+
| name         |
+--------------+
| today        |
| todayofmonth |
| todayofweek  |
| todayofyear  |
+--------------+
```
