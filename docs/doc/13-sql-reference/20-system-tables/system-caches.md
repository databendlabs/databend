---
title: system.caches
---

An overview of various caches being managed in Databend. 

The table below shows the cache name, the number of items in the cache, and the size of the cache:
```sql
SELECT * FROM system.caches;
+--------------------------------+-----------+------+
| name                           | num_items | size |
+--------------------------------+-----------+------+
| table_snapshot_cache           |         2 |    2 |
| table_snapshot_statistic_cache |         0 |    0 |
| segment_info_cache             |        64 |   64 |
| bloom_index_filter_cache       |         0 |    0 |
| bloom_index_meta_cache         |         0 |    0 |
| prune_partitions_cache         |         2 |    2 |
| file_meta_data_cache           |         0 |    0 |
+--------------------------------+-----------+------+
```