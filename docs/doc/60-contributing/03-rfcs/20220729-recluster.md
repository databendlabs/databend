---
title: Optimize table recluster
description: 
  RFC for recluster a clustered table
---

## Summary

Clustering is inspired by data clustering in Snowflake and attribute clustering in Oracle.

A clustered table stores data in an ordered way based on the values of a certain set of columns in the table. Clustering is beneficial to partition elimination and file defragmentation.

By default, data is stored in tables according to natural dimensions. We need to recluster the tables by the cluster key. On the other hand, even though the clustered table has been well-clustered, clustering will become worse over time if new data is constantly written. Therefore, it is necessary to add recluster operation.

## Design

For more detailed principles and pictures, please refer to [snowflake auto clustering](https://sundy-li.github.io/posts/探索snowflake-auto-clustering/).

The cost of performing full table sorting is very expensive, especially for the tables that constantly have new data inflow. In order to make a balance between efficient pruning and low cost, the tables only need to be roughly sorted instead of fully sorted. Therefore, two metrics are introduced in [Metrics](#metrics) to determine whether the table is well clustered. The goal of recluster is to reduce `overlap` and `depth`.

To avoid churning on the same piece of data many times, we divides the blocks into different levels like LSM trees. The recluster is similar to the LSM compact operation. The `level` represents the number of times the data in that block has been clustered. The recluster operation is performed on the same level.

```rust
pub struct ClusterStatistics {
    ... ...
    pub level: i32,
}
```

The workflow of a recluster operation is divided into two tasks, block selection and block merge.

### syntax

```sql
optimize table tbl_name recluster 
```

Optimization is performed until the table is well clustered enough.

The optimize statement should be triggered by DML on the table.

### Metrics

- overlap
  The number of blocks that overlap with a given block.

- depth
  The number of blocks that overlap at the same point. The points are collected from the minimum and maximum value in the clustering values domain range.

### Block Selection

The initial level of newly incoming data is 0. We focus on the newer data first, in other words the selection operations are preferentially performed on level 0. The advantage of doing this is to reduce write amplification.

1. Calculate the depth of each point and the overlaps of the block, and summarize to get avg_depth. The algorithm has already been reflected in [system$clustering_information](https://github.com/datafuselabs/databend/pull/5426), and will not be repeated here. The ideal result for avg_depth is 1. In order to achieve roughly ordering, consider defining a threshold or a ratio (threshold = blocks_num * ratio). As long as avg_depth is not greater than this threshold, the blocks at this level can be considered well-clustered, then we perform block selection on the next level.

2. Select the point range (one or more) with the highest depth, and select the blocks covered by the range as a set of objects for the next block-merge. If there is more than one range with the highest depth, there may be multiple sets of blocks that can be parallelized during block-merge.

Tip:
```
1. The cluster key may be created or altered when the table has data, so there may be blocks that are not sorted according to the cluster key. Consider temporarily ignoring such blocks when doing recluster.

2. If the cluster key of a block has only one value (the maximum and minimum values are equal, reaching the constant state) and its row_num is 1000_000, set its level to -1 and filter it out when doing recluster.

3. The selected blocks maybe need to consider the total size, otherwise the sorting may be out of memory.
```

### Block Merge

Sort and merge the collected blocks. After the merged block exceeds a certain threshold (1000_000 rows), it will be divided into multiple blocks. The newly generated block is put into the next level.

Organize the blocks and generate new segments and snapshot, and finally update table meta. If there is a new DML execution during this period, the current workflow will fail to commit and return an error. We need to consider the specific processing flow later.

The selection and merge operation is repeated until the table is well clustered enough.
