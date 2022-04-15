---
title: SHOW CREATE DATABASE
---

Shows the CREATE DATABASE statement that creates the named database.

## Syntax

```
SHOW CREATE DATABASE database_name
```

## Examples

```sql
mysql> SHOW CREATE DATABASE default;
+----------+---------------------------+
| Database | Create Database           |
+----------+---------------------------+
| default  | CREATE DATABASE `default` |
+----------+---------------------------+
```
