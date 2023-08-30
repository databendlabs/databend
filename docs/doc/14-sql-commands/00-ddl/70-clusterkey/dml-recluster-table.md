---
title: RECLUSTER TABLE
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.2.25"/>

Re-clusters a table. For why and when to re-cluster a table, see [Re-clustering Table](index.md#re-clustering-table).

### Syntax

```sql
ALTER TABLE [IF EXISTS] <table_name> RECLUSTER [FINAL] [WHERE condition] [LIMIT <segment_count>]
```

The command has a limitation on the number of segments it can process, with the default value being "max_thread * 4". You can modify this limit by using the **LIMIT** option. Alternatively, you have two options to cluster your data in the table further:

- Run the command multiple times against the table.
- Use the **FINAL** option to continuously optimize the table until it is fully clustered.

:::note

Re-clustering a table consumes time (even longer if you include the **FINAL** option) and credits (when you are in Databend Cloud). During the optimizing process, do NOT perform DML actions to the table.
:::

The command does not cluster the table from the ground up. Instead, it selects and reorganizes the most chaotic existing storage blocks from the latest **LIMIT** segments using a clustering algorithm. For more information about how the re-clustering works, see https://databend.rs/doc/contributing/rfcs/recluster.

### Examples

```sql
-- create table
create table t(a int, b int) cluster by(a+1);

-- insert some data to t
insert into t values(1,1),(3,3);
insert into t values(2,2),(5,5);
insert into t values(4,4);

select * from clustering_information('default','t')\G
*************************** 1. row ***************************
            cluster_key: ((a + 1))
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
            cluster_key: ((a + 1))
      total_block_count: 2
   constant_block_count: 1
unclustered_block_count: 0
       average_overlaps: 1.0
          average_depth: 2.0
  block_depth_histogram: {"00002":2}
```