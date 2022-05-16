---
title: RENAME DATABASE
---

Changes the name of a database.

## Syntax

```sql
ALTER DATABASE [ IF EXISTS ] <name> RENAME TO <new_db_name>
```

## Examples

```sql
CREATE DATABASE DATABEND;
```

```sql
SHOW DATABASES;
+--------------------+
| Database           |
+--------------------+
| DATABEND           |
| INFORMATION_SCHEMA |
| default            |
| system             |
+--------------------+
```

```sql
ALTER DATABASE `DATABEND` RENAME TO `NEW_DATABEND`;
```

```sql
SHOW DATABASES;
+--------------------+
| Database           |
+--------------------+
| INFORMATION_SCHEMA |
| NEW_DATABEND       |
| default            |
| system             |
+--------------------+
```
