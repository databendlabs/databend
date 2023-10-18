---
title: TRUNCATE TABLE
---

Removes all data from a table while preserving the table's schema. It deletes all rows in the table, making it an empty table with the same columns and constraints. Please note that, it does not release the disk space allocated to the table.

See also: [DROP TABLE](20-ddl-drop-table.md)

## Syntax

```sql
TRUNCATE TABLE [db.]table_name
```

## Examples

```sql
root@localhost> CREATE TABLE test_truncate(a BIGINT UNSIGNED, b VARCHAR);
Processed in (0.027 sec)

root@localhost> INSERT INTO test_truncate(a,b) VALUES(1234, 'databend');
1 rows affected in (0.060 sec)

root@localhost> SELECT * FROM test_truncate;

SELECT
  *
FROM
  test_truncate

┌───────────────────┐
│    a   │     b    │
│ UInt64 │  String  │
├────────┼──────────┤
│   1234 │ databend │
└───────────────────┘
1 row in 0.019 sec. Processed 1 rows, 1B (53.26 rows/s, 17.06 KiB/s)

root@localhost> TRUNCATE TABLE test_truncate;

TRUNCATE TABLE test_truncate

0 row in 0.047 sec. Processed 0 rows, 0B (0 rows/s, 0B/s)

root@localhost> SELECT * FROM test_truncate;

SELECT
  *
FROM
  test_truncate

0 row in 0.017 sec. Processed 0 rows, 0B (0 rows/s, 0B/s)
```
