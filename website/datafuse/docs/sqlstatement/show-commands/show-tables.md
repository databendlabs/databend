---
id: show-tables
title: SHOW TABLES
---

Shows the list of tables in the currently selected database.

## Syntax

```
SHOW TABLES  [LIKE 'pattern' | WHERE expr]
```

## Examples

```
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
```
mysql> SHOW TABLES LIKE 'numbers%';
+---------------+
| name          |
+---------------+
| numbers_local |
+---------------+
```

Showing the tables begin with `"numbers"`:
```
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
```
mysql> SHOW TABLES WHERE name LIKE 'numbers%';
+---------------+
| name          |
+---------------+
| numbers       |
| numbers_local |
| numbers_mt    |
+---------------+
```