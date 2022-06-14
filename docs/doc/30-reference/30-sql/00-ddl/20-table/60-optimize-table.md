---
title: OPTIMIZE TABLE
---

Optimize the given table, by compacting data, and purging historical data.

## Syntax

```
OPTIMIZE TABLE [database.]table_name [ PUREGE | COMPACT | ALL] 
```

- `OPTIMIZE TABLE T PUREGE`

  Purges the historical data of table T, only the last snapshot, and the data(segments/blocks) referenced by this snapshot will be kept.

  If data keeps being injected into table at small scale, and historical data is not required, it is recommended to execute this statement periodically.

 
- `OPTIMIZE TABLE T COMPACT`
 
  Compact the table data only by merging small blocks/segments into larger ones.
 
  - A new snapshot of table T will be added to the history, by this compaction operation.

  - Depends on the size of the given table, it may take quite a while to complete the execution.

 
- `optimize table T ALL`
 
  Compact the historical data, and then, purge the history 

- `optimize table T `

   The same as `optimize table T purge`




## Examples

```sql
mysql> use default;
mysql> create table t as select * from numbers(10000000);
mysql> select snapshot_id, segment_count, block_count, row_count from fuse_snapshot('default', 't');
+----------------------------------+---------------+-------------+-----------+
| snapshot_id                      | segment_count | block_count | row_count |
+----------------------------------+---------------+-------------+-----------+
| f150212c302244fa9780cc6337138c6e |            16 |          16 |  10000000 |
+----------------------------------+---------------+-------------+-----------+

mysql> -- small insertions;
mysql> insert into t values(1);
mysql> insert into t values(2);
mysql> insert into t values(3);
mysql> insert into t values(4);
mysql> insert into t values(5);

mysql> -- for each insertion, a new snapshot is generated
mysql> select snapshot_id, segment_count, block_count, row_count from fuse_snapshot('default', 't');
+----------------------------------+---------------+-------------+-----------+
| snapshot_id                      | segment_count | block_count | row_count |
+----------------------------------+---------------+-------------+-----------+
| 51a03a3d0e1241529097f7a2f20b76eb |            21 |          21 |  10000005 |
| f27c19e1c85046f6a73403de846f9483 |            20 |          20 |  10000004 |
| 9baba3bbe0244aab9d1b25f5b69f7378 |            19 |          19 |  10000003 |
| d90010f11b574b078becbc6b6791703e |            18 |          18 |  10000002 |
| f1c1f24d9d3742b9a05c827a3f03e0e0 |            17 |          17 |  10000001 |
| f150212c302244fa9780cc6337138c6e |            16 |          16 |  10000000 |
+----------------------------------+---------------+-------------+-----------+
mysql> -- newly generated blocks are too small, let's compact them 
mysql> optimize table t compact;
mysql> select snapshot_id, segment_count, block_count, row_count from fuse_snapshot('default', 't');
+----------------------------------+---------------+-------------+-----------+
| snapshot_id                      | segment_count | block_count | row_count |
+----------------------------------+---------------+-------------+-----------+
| 4f33a63031424ed095b8c2f9e8b15ecb |            16 |          16 |  10000005 | // <- the new snapshot
| 51a03a3d0e1241529097f7a2f20b76eb |            21 |          21 |  10000005 |
| f27c19e1c85046f6a73403de846f9483 |            20 |          20 |  10000004 |
| 9baba3bbe0244aab9d1b25f5b69f7378 |            19 |          19 |  10000003 |
| d90010f11b574b078becbc6b6791703e |            18 |          18 |  10000002 |
| f1c1f24d9d3742b9a05c827a3f03e0e0 |            17 |          17 |  10000001 |
| f150212c302244fa9780cc6337138c6e |            16 |          16 |  10000000 |
+----------------------------------+---------------+-------------+-----------+

mysql> -- If we do not care about the historical data  
mysql> optimize table t purge;
Query OK, 0 rows affected (0.08 sec)
Read 0 rows, 0.00 B in 0.041 sec., 0 rows/sec., 0.00 B/sec.

mysql> select snapshot_id, segment_count, block_count, row_count from fuse_snapshot('default', 't');
+----------------------------------+---------------+-------------+-----------+
| snapshot_id                      | segment_count | block_count | row_count |
+----------------------------------+---------------+-------------+-----------+
| 4f33a63031424ed095b8c2f9e8b15ecb |            16 |          16 |  10000005 |
+----------------------------------+---------------+-------------+-----------+
```
