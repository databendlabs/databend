---
title: CLUSTERING_INFORMATION
---

Returns clustering information of a table.

## Syntax

```sql
CLUSTERING_INFORMATION('<database_name>', '<table_name>')
```

## Examples

```sql
CREATE TABLE mytable(a int, b int) CLUSTER BY(a+1);

INSERT INTO mytable VALUES(1,1),(3,3);
INSERT INTO mytable VALUES(2,2),(5,5);
INSERT INTO mytable VALUES(4,4);

SELECT * FROM CLUSTERING_INFORMATION('default','mytable')\G
*************************** 1. row ***************************
            cluster_key: ((a + 1))
      total_block_count: 3
   constant_block_count: 1
unclustered_block_count: 0
       average_overlaps: 1.3333
          average_depth: 2.0
  block_depth_histogram: {"00002":3}
```

| Parameter               	| Description                                                                                                            	|
|-------------------------	|------------------------------------------------------------------------------------------------------------------------	|
| cluster_key         	| The defined cluster key.                                                                                               	|
| total_block_count       	| The current count of blocks.                                                                                           	|
| constant_block_count    	| The count of blocks where min/max values are equal, meaning each block contains only one (group of) cluster_key value. 	|
| unclustered_block_count 	| The count of blocks that have not yet been clustered.                                                                  	|
| average_overlaps        	| The average ratio of overlapping blocks within a given range.                                                          	|
| average_depth           	| The average depth of overlapping partitions for the cluster key.                                                       	|
| block_depth_histogram   	| The number of partitions at each depth level. A higher concentration of partitions at lower depths indicates more effective table clustering.                                                                          	|