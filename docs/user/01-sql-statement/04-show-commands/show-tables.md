---
title: SHOW TABLES
---

Shows the list of tables in the currently selected database.

## Syntax

```
SHOW TABLES  [LIKE 'pattern' | WHERE expr | FROM 'pattern' | IN 'pattern']
```

## Examples

```sql
mysql> SHOW TABLES;
+---------------+
| name          |
+---------------+
| clusters      |
| contributors  |
| databases     |
| functions     |
| numbers       |
| numbers_local |
| numbers_mt    |
| one           |
| processes     |
| settings      |
| tables        |
| tracing       |
+---------------+
```

Showing the tables with table name `"numbers_local"`:
```sql
mysql> SHOW TABLES LIKE 'numbers_local';
+---------------+
| name          |
+---------------+
| numbers_local |
+---------------+
```

Showing the tables begin with `"numbers"`:
```sql
mysql> SHOW TABLES LIKE 'numbers%';
+---------------+
| name          |
+---------------+
| numbers       |
| numbers_local |
| numbers_mt    |
+---------------+
```

Showing the tables begin with `"numbers"` with `WHERE`:
```sql
mysql> SHOW TABLES WHERE name LIKE 'numbers%';
+---------------+
| name          |
+---------------+
| numbers       |
| numbers_local |
| numbers_mt    |
+---------------+
```

Showing the tables are inside `"ss"`:
```sql
mysql> SHOW TABLES FROM 'ss';
+---------------+
| name          |
+---------------+
| numbers       |
| numbers_local |
| numbers_mt    |
+---------------+
```
