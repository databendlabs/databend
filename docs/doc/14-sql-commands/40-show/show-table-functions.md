---
title: SHOW TABLE_FUNCTIONS
---

Shows the list of supported table functions currently.

## Syntax

```
SHOW TABLE_FUNCTIONS  [LIKE 'pattern' | WHERE expr]
```

## Example

```sql
SHOW TABLE_FUNCTIONS;
+------------------------+
| name                   |
+------------------------+
| numbers                |
| numbers_mt             |
| numbers_local          |
| fuse_snapshot          |
| fuse_segment           |
| fuse_block             |
| fuse_statistic         |
| clustering_information |
| sync_crash_me          |
| async_crash_me         |
| infer_schema           |
+------------------------+
```

Showing the table functions begin with `"number"`:
```sql
SHOW TABLE_FUNCTIONS LIKE 'number%';
+---------------+
| name          |
+---------------+
| numbers       |
| numbers_mt    |
| numbers_local |
+---------------+
```

Showing the table functions begin with `"number"` with `WHERE`:
```sql
SHOW TABLE_FUNCTIONS WHERE name LIKE 'number%';
+---------------+
| name          |
+---------------+
| numbers       |
| numbers_mt    |
| numbers_local |
+---------------+
```
