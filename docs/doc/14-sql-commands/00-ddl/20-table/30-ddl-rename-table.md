---
title: RENAME TABLE
---

Changes the name of a table.

## Syntax

```sql
ALTER TABLE [ IF EXISTS ] <name> RENAME TO <new_table_name>
```

## Examples

```sql
CREATE TABLE test(a INT);
```

```sql
SHOW TABLES;
+------+
| name |
+------+
| test |
+------+
```

```sql
ALTER TABLE `test` RENAME TO `new_test`;
```

```sql
SHOW TABLES;
+----------+
| name     |
+----------+
| new_test |
+----------+
```
