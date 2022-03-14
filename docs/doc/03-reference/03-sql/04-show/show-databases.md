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
mysql> SHOW DATABASES;
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
9 rows in set (0.00 sec)
```

Showing the databases with database `"ss"`:
```sql
mysql> SHOW DATABASES WHERE Database = 'ss';
+----------+
| Database |
+----------+
| ss       |
+----------+
1 row in set (0.01 sec)
```

Showing the databases begin with `"ss"`:
```sql
mysql> SHOW DATABASES Like 'ss%';
+----------+
| Database |
+----------+
| ss       |
| ss1      |
| ss2      |
| ss3      |
+----------+
4 rows in set (0.01 sec)
```

Showing the databases begin with `"ss"` with where:
```sql
mysql> SHOW DATABASES WHERE Database Like 'ss%';
+----------+
| Database |
+----------+
| ss       |
| ss1      |
| ss2      |
| ss3      |
+----------+
4 rows in set (0.01 sec)
```

Showing the databases like substring expr:
```sql
mysql> SHOW DATABASES Like SUBSTRING('ss%' FROM 1 FOR 3);
+----------+
| Database |
+----------+
| ss       |
| ss1      |
| ss2      |
| ss3      |
+----------+
4 rows in set (0.01 sec)
```

Showing the databases like substring expr with where:
```sql
mysql> SHOW DATABASES WHERE Database Like SUBSTRING('ss%' FROM 1 FOR 3);
+----------+
| Database |
+----------+
| ss       |
| ss1      |
| ss2      |
| ss3      |
+----------+
4 rows in set (0.01 sec)
```
