---
title: RECLUSTER TABLE
---

Use this command to re-cluster the data in a clustered table.

A well-clustered table may become chaotic in some storage blocks negatively affecting the query performance. For example, the table continues to have DML operations (INSERT / UPDATE / DELETE). This command helps reduce the chaos by re-clustering the table.

The re-clustering operation does not cluster the table from the ground up. It selects and reorganizes the most chaotic existing storage blocks by calculating based on the clustering algorithm. For more information about how the re-clustering works, see https://databend.rs/doc/contributing/rfcs/recluster.

You can run the command against a table multiple times to further cluster your data in the table. Alternatively, you can use the FINAL option to keep optimizing the table until it is fully clustered. 

Please note that re-clustering a table consumes time (even longer if you include the FINAL option) and credits (when you are in Databend Cloud). During the optimizing process, do not perform DML actions to the table.

## Syntax

```sql
ALTER TABLE [IF EXISTS] <name> RECLUSTER [FINAL] [WHERE condition]
```

## Examples

```sql
-- create table
create table t(a int, b int) cluster by(a+1);

-- insert some data to t
insert into t values(1,1),(3,3);
insert into t values(2,2),(5,5);
insert into t values(4,4);

select * from clustering_information('default','t')\G
*************************** 1. row ***************************
        cluster_by_keys: ((a + 1))
      total_block_count: 3
   constant_block_count: 1
unclustered_block_count: 0
       average_overlaps: 1.3333
          average_depth: 2.0
  block_depth_histogram: {"00002":3}

-- alter table recluster
ALTER TABLE t RECLUSTER FINAL WHERE a != 4;

select * from clustering_information('default','t')\G
*************************** 1. row ***************************
        cluster_by_keys: ((a + 1))
      total_block_count: 2
   constant_block_count: 1
unclustered_block_count: 0
       average_overlaps: 1.0
          average_depth: 2.0
  block_depth_histogram: {"00002":2}
```