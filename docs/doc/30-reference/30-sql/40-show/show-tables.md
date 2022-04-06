---
title: SHOW TABLES
---

Shows the list of tables in the currently selected database.

## Syntax

```
SHOW [FULL] TABLES  [LIKE 'pattern' | WHERE expr | FROM 'pattern' | IN 'pattern']
```

## Examples

```sql
mysql> SHOW TABLES;
+------------------+
| Tables_in_system |
+------------------+
| clusters         |
| columns          |
| configs          |
| contributors     |
| credits          |
| databases        |
| engines          |
| functions        |
| metrics          |
| one              |
| processes        |
| query_log        |
| roles            |
| settings         |
| tables           |
| tracing          |
| users            |
| warehouses       |
+------------------+
```

Showing the tables with table name `"numbers_local"`:
```sql
mysql> SHOW TABLES LIKE 'settings';
+------------------+
| Tables_in_system |
+------------------+
| settings         |
+------------------+
```

Showing the tables begin with `"numbers"`:
```sql
mysql> SHOW TABLES LIKE 'co%';
+------------------+
| Tables_in_system |
+------------------+
| columns          |
| configs          |
| contributors     |
+------------------+
```

Showing the tables begin with `"numbers"` with `WHERE`:
```sql
mysql> SHOW TABLES WHERE table_name LIKE 'co%';
+------------------+
| Tables_in_system |
+------------------+
| columns          |
| configs          |
| contributors     |
+------------------+
```

Showing the tables are inside `"ss"`:
```sql
mysql> SHOW TABLES FROM 'system';
+------------------+
| Tables_in_system |
+------------------+
| clusters         |
| columns          |
| configs          |
| contributors     |
| credits          |
| databases        |
| engines          |
| functions        |
| metrics          |
| one              |
| processes        |
| query_log        |
| roles            |
| settings         |
| tables           |
| tracing          |
| users            |
| warehouses       |
+------------------+
```
