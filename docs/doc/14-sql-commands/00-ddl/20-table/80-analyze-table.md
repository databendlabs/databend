---
title: ANALYZE TABLE
---

The objective of analyzing a table in Databend is to calculate table statistics, such as distinct number of columns.

## What is Table statistic file?

A table statistic file is a JSON file that save table statistic data, such as distinct values of table column.

Databend creates a unique ID for each database and table for storing the table statistic file and saves them to your object storage in the path `<bucket_name>/[root]/<db_id>/<table_id>/`. Each table statistic file is named with a UUID (32-character lowercase hexadecimal string).

| File     | Format  | Filename                        | Storage Folder                                                               |
|----------|---------|---------------------------------|----------------------------------------------------------------------------|
| Table statistic | JSON    | `<32bitUUID>_<version>.json`    | `<bucket_name>/[root]/<db_id>/<table_id>/_ts/`   |

## Syntax
```sql
ANALYZE TABLE [database.]table_name
```

- `ANALYZE TABLE <table_name>`

    Estimates the number of distinct values of each column in a table. 
    
    - It does not display the estimated results after execution. To show the estimated results, use the function [FUSE_STATISTIC](../../../15-sql-functions/111-system-functions/fuse_statistic.md).
    - The command does not identify distinct values by comparing them but by counting the number of storage segments and blocks. This might lead to a significant difference between the estimated results and the actual value, for example, multiple blocks holding the same value. In this case, Databend recommends compacting the storage segments and blocks to merge them as much as possible before you run the estimation.

## Examples

This example estimates the number of distinct values for each column in a table and shows the results with the function FUSE_STATISTIC:

```sql
create table t(a uint64);

insert into t values (5);
insert into t values (6);
insert into t values (7);

select * from t order by a;

----
5
6
7

-- FUSE_STATISTIC will not return any results until you run an estimation with OPTIMIZE TABLE.
select * from fuse_statistic('db_09_0020', 't');

analyze table `t`;

select * from fuse_statistic('db_09_0020', 't');

----
(0,3);


insert into t values (5);
insert into t values (6);
insert into t values (7);

select * from t order by a;

----
5
5
6
6
7
7

-- FUSE_STATISTIC returns results of your last estimation. To get the most recent estimated values, run the estimation again.
-- OPTIMIZE TABLE does not identify distinct values by comparing them but by counting the number of storage segments and blocks.
select * from fuse_statistic('db_09_0020', 't');

----
(0,3);

analyze table `t`;

select * from fuse_statistic('db_09_0020', 't');

----
(0,6);

-- Best practice: Compact the table before running the estimation.
optimize table t compact;

analyze table `t`;

select * from fuse_statistic('db_09_0020', 't');

----
(0,3);
```