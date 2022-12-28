---
title: OPTIMIZE TABLE
---

The objective of optimizing a table in Databend is to compact or purge its historical data in your object storage. This helps save storage space and improve query efficiency.

:::caution
Databend's Time Travel feature relies on historical data. If you purge historical data from a table with the command `OPTIMIZE TABLE <your_table> PURGE` or `OPTIMIZE TABLE <your_table> ALL`, the table will not be eligible for time travel. The command removes all snapshots (except the most recent one) and their associated segments,block files and table statistic file.
:::

## What are Snapshot, Segment, Block?

Snapshot, segment, and block are the concepts Databend uses for data storage. Databend uses them to construct a hierarchical structure for storing table data.

![](/img/sql/storage-structure.PNG)

Databend automatically creates snapshots of a table when data updates occur, so a snapshot represents a version of the table's data. When working with Databend, you're most likely to access a snapshot with the snapshot ID when you retrieve and query a previous version of the table' data with the [AT](../../20-query-syntax/dml-at.md) clause.

A snapshot is a JSON file that does not save the table's data but indicate the segments the snapshot links to. If you run [FUSE_SNAPSHOT](../../../15-sql-functions/111-system-functions/fuse_snapshot.md) against a table, you can find the saved snapshots for the table.

A segment is a JSON file that organizes the storage blocks (at least 1, at most 1,000) where the data is stored. If you run [FUSE_SEGMENT](../../../15-sql-functions/111-system-functions/fuse_segment.md) against a snapshot with the snapshot ID, you can find which segments are referenced by the snapshot.

Databends saves actual table data in parquet files and considers each parquet file as a block. If you run [FUSE_BLOCK](../../../15-sql-functions/111-system-functions/fuse_block.md) against a snapshot with the snapshot ID, you can find which blocks are referenced by the snapshot.

Databend creates a unique ID for each database and table for storing the snapshot, segment, and block files and saves them to your object storage in the path `<bucket_name>/[root]/<db_id>/<table_id>/`. Each snapshot, segment, and block file is named with a UUID (32-character lowercase hexadecimal string).

| File     | Format  | Filename                        | Storage Folder                                                               |
|----------|---------|---------------------------------|----------------------------------------------------------------------------|
| Snapshot | JSON    | `<32bitUUID>_<version>.json`    | `<bucket_name>/[root]/<db_id>/<table_id>/_ss/`   |
| Segment  | JSON    | `<32bitUUID>_<version>.json`    | `<bucket_name>/[root]/<db_id>/<table_id>/_sg/`   |
| Block    | parquet | `<32bitUUID>_<version>.parquet` | `<bucket_name>/[root]/<db_id>/<table_id>/_b/` |

## Table Optimization Considerations

Consider optimizing a table regularly if the table receives frequent updates. Databend recommends these best practices to help decide on an optimization for a table:

### When to Optimize

If the blocks of a table meets the following conditions, the table requires an optimization:

1. The number of blocks that meet the following conditions is greater than 100:

- The block size is less than 100M.
- The number of the rows in the block is less than 800,000.

```sql
select if(count(*)>100,'The table needs compact now','The table does not need compact now') from fuse_block('<your_database>','<your_table>') where file_size <100*1024*1024 and row_count<800000;
```

### Compacting Segments Only

The optimization merges both segments and blocks of a table by default. However, you can choose to compact the segments only when a table has too many segments.

Databend recommends optimizing the segments only for a table when the table has more than 1,000 segments and the average number of blocks per segment is less than 500.

```sql
select count(*),avg(block_count),if(avg(block_count)<500,'The table needs segment compact now','The table does not need segment compact now') from fuse_segment('<your_database>','<your_table>');
```

### When NOT to Optimize

Optimizing a table could be time-consuming, especially for large ones. Databend does not recommend optimizing a table too frequently. Before you optimize a table, make sure the table fully satisfies the conditions described in [When to Optimize](#when-to-optimize).

## Syntax

```sql
OPTIMIZE TABLE [database.]table_name [ PURGE | COMPACT | ALL | [SEGMENT] [LIMIT <segment_count>]
```

- `OPTIMIZE TABLE <table_name> PURGE`

    Purges the historical data of table. Only the latest snapshot (including the segments, blocks and table statistic file referenced by this snapshot) will be kept.
    (For more explanations of table statistic file, see [ANALYZE TABLE](./80-analyze-table.md).)

- `OPTIMIZE TABLE <table_name> COMPACT [LIMIT <segment_count>]`

    Compacts the table data by merging small blocks and segments into larger ones.

    - This command creates a new snapshot (along with compacted segments and blocks) of the most recent table data without affecting the existing storage files, so the storage space won't be released until you run `OPTIMIZE TABLE <table_name> PURGE`.
    - Depending on the size of the given table, it may take quite a while to complete the execution.
    - The option LIMIT sets the maximum number of segments to be compacted. In this case, Databend will select and compact the latest segments.

-  `OPTIMIZE TABLE <table_name> COMPACT SEGMENT [LIMIT <segment_count>]`

    Compacts the table data by merging small segments into larger ones.

    - See [Compacting Segments Only](#compacting-segments-only) for when you need this command.
    - The option LIMIT sets the maximum number of segments to be compacted. In this case, Databend will select and compact the latest segments.

- `OPTIMIZE TABLE <table_name> ALL`

    Equals to `OPTIMIZE TABLE <table_name> COMPACT` + `OPTIMIZE TABLE <table_name> PURGE`。

- `OPTIMIZE TABLE <table_name>`

    Works the same way as `OPTIMIZE TABLE <table_name> PURGE`.

## Examples

This example compacts and purges historical data from a table:

```sql
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