---
title: FUSE_SNAPSHOT
---

Returns the snapshot information of a table for querying previous versions of your data. For more information, see [AT](../../30-sql/20-query-syntax/dml-at.md).

## Syntax

```sql
FUSE_SNAPSHOT('<database_name>', '<table_name>')
```

## Examples

```sql
CREATE TABLE mytable(a int, b int) CLUSTER BY(a+1);

INSERT INTO mytable VALUES(1,1),(3,3);
INSERT INTO mytable VALUES(2,2),(5,5);
INSERT INTO mytable VALUES(4,4);

SELECT * FROM FUSE_SNAPSHOT(‘default‘,’mytable‘);

---
| snapshot_id                      | snapshot_location                                          | format_version | previous_snapshot_id             | segment_count | block_count | row_count | bytes_uncompressed | bytes_compressed | timestamp                  |
|----------------------------------|------------------------------------------------------------|----------------|----------------------------------|---------------|-------------|-----------|--------------------|------------------|----------------------------|
| dd98266f968f4817b470255592d04fda | 670655/670675/_ss/dd98266f968f4817b470255592d04fda_v1.json | 1              | \N                               | 1             | 1           | 2         | 16                 | 290              | 2022-09-07 01:58:55.204997 |
| 2f2d004ff6f842cdb25f5631b2bbb703 | 670655/670675/_ss/2f2d004ff6f842cdb25f5631b2bbb703_v1.json | 1              | dd98266f968f4817b470255592d04fda | 2             | 2           | 4         | 32                 | 580              | 2022-09-07 01:59:09.742999 |
| 0aa6dfd5d5364bde80f21161ba48c96e | 670655/670675/_ss/0aa6dfd5d5364bde80f21161ba48c96e_v1.json | 1              | 2f2d004ff6f842cdb25f5631b2bbb703 | 3             | 3           | 5         | 40                 | 862              | 2022-09-07 01:59:16.858454 |
```