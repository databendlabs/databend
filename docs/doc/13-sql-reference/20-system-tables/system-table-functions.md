---
title: system.table_functions
---

Get all table functions' names.

```sql
SELECT * FROM system.table_functions;
``` 

```text
+----------------------------+
| name                       |
+----------------------------+
| numbers                    |
| numbers_mt                 |
| numbers_local              |
| fuse_snapshot              |
| fuse_segment               |
| fuse_block                 |
| fuse_column                |
| fuse_statistic             |
| clustering_information     |
| sync_crash_me              |
| async_crash_me             |
| infer_schema               |
| list_stage                 |
| generate_series            |
| range                      |
| ai_to_sql                  |
| execute_background_job     |
| license_info               |
| suggested_background_tasks |
| tenant_quota               |
| fuse_encoding              |
+----------------------------+
```