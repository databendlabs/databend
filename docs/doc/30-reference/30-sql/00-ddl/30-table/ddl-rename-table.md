---
title: RENAME TABLE
---

Changes the name of a table.

## Syntax

```sql
ALTER TABLE [ IF EXISTS ] <name> RENAME TO <new_table_name>
```

## Examples

```sql title='mysql>'
create table test(a int);
```

```sql title='mysql>'
show tables;
```

```sql
+------+
| name |
+------+
| test |
+------+
```

```sql title='mysql>'
alter table `test` rename to `new_test`;
```

```sql title='mysql>'
show tables;
```

```sql
+----------+
| name     |
+----------+
| new_test |
+----------+
```
