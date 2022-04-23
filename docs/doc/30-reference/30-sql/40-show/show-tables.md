---
title: SHOW TABLES
---

Shows the list of tables in the currently selected database.

## Syntax

```
SHOW [EXTENDED] [FULL] TABLES
    [{FROM | IN} db_name]
    [LIKE 'pattern' | WHERE expr]
```

## Examples

```sql
SHOW TABLES;
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

Showing the tables with table name `"settings"`:
```sql
SHOW TABLES LIKE 'settings';
+------------------+
| Tables_in_system |
+------------------+
| settings         |
+------------------+
```

Showing the tables begin with `"co"`:
```sql
SHOW TABLES LIKE 'co%';
+------------------+
| Tables_in_system |
+------------------+
| columns          |
| configs          |
| contributors     |
+------------------+
```

Showing the tables begin with `"co"` with `WHERE`:
```sql
SHOW TABLES WHERE table_name LIKE 'co%';
+------------------+
| Tables_in_system |
+------------------+
| columns          |
| configs          |
| contributors     |
+------------------+
```

Showing the tables are inside `"system"`:
```sql
SHOW TABLES FROM 'system';
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
