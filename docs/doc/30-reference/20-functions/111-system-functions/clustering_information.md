---
title: CLUSTERING_INFORMATION
---

Returns clustering information of a table.

## Syntax

```sql
CLUSTERING_INFORMATION(‘<database_name>’, ‘<table_name>’)
```

## Examples

```sql
CREATE TABLE mytable(a int, b int) CLUSTER BY(a+1);

INSERT INTO mytable VALUES(1,1),(3,3);
INSERT INTO mytable VALUES(2,2),(5,5);
INSERT INTO mytable VALUES(4,4);

SELECT * FROM CLUSTERING_INFORMATION(‘default‘,’mytable‘);

---
| cluster_by_keys | total_block_count | total_constant_block_count | average_overlaps | average_depth | block_depth_histogram |
|-----------------|-------------------|----------------------------|------------------|---------------|-----------------------|
| ((a + 1))       | 3                 | 1                          | 1.3333           | 2.0           | {"00002":3}           |
```