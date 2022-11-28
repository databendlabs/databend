---
title: SHOW DATABASES
---

Shows the list of databases that exist on the instance.

## Syntax

```
SHOW DATABASES [LIKE expr | WHERE expr]
```

## Examples
```sql
SHOW DATABASES;
+----------+
| Database |
+----------+
| default  |
| for_test |
| local    |
| ss       |
| ss1      |
| ss2      |
| ss3      |
| system   |
| test     |
+----------+
```

Showing the databases with database `"ss"`:
```sql
SHOW DATABASES WHERE Database = 'ss';
+----------+
| Database |
+----------+
| ss       |
+----------+
```

Showing the databases begin with `"ss"`:
```sql
SHOW DATABASES Like 'ss%';
+----------+
| Database |
+----------+
| ss       |
| ss1      |
| ss2      |
| ss3      |
+----------+
```

Showing the databases begin with `"ss"` with where:
```sql
SHOW DATABASES WHERE Database Like 'ss%';
+----------+
| Database |
+----------+
| ss       |
| ss1      |
| ss2      |
| ss3      |
+----------+
```

Showing the databases like substring expr:
```sql
SHOW DATABASES Like SUBSTRING('ss%' FROM 1 FOR 3);
+----------+
| Database |
+----------+
| ss       |
| ss1      |
| ss2      |
| ss3      |
+----------+
```

Showing the databases like substring expr with where:
```sql
SHOW DATABASES WHERE Database Like SUBSTRING('ss%' FROM 1 FOR 3);
+----------+
| Database |
+----------+
| ss       |
| ss1      |
| ss2      |
| ss3      |
+----------+
```
