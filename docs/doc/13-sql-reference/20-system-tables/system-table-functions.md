---
title: system.functions
---

Get all table functions' names.

```sql
SELECT * FROM system.table_functions;
``` 

```text
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