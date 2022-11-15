---
title: SHOW STAGES
sidebar_label: SHOW STAGES
---

Returns a list of the created stages. The output list does not include the [User Stage](index.md#user-stage).

## Syntax

```sql
SHOW STAGES;
```

## Examples

```sql
SHOW STAGES;

---
name|stage_type|number_of_files|creator           |comment|
----+----------+---------------+------------------+-------+
eric|Internal  |              0|'root'@'127.0.0.1'|       |
```