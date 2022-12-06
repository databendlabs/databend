---
title: RESTORE TABLE
---

Restores a table to an earlier version with a snapshot ID or timestamp.

By the snapshot ID or timestamp you specify in the command, Databend restores the table to a prior state where the snapshot was created. To retrieve snapshot IDs and timestamps of a table, use [FUSE_SNAPSHOT](../../../15-sql-functions/111-system-functions/fuse_snapshot.md).

The capability to restore a table is subject to these conditions:

- The command only restores existing tables to their prior states. To recover a dropped table, use [UNDROP TABLE](21-ddl-undrop-table.md).

- Restoring a table is part of Databend's time travel feature. Before using the command, make sure the table you want to restore is eligible for time travel. For example, the command doesn't work for transient tables because Databend does not create or store snapshots for such tables.

- You cannot roll back after restoring a table to a prior state, but you can restore the table again to an earlier state.

- Databend recommends this command for emergency recovery only. To query the history data of a table, use the [AT](../../20-query-syntax/dml-at.md) clause.

## Syntax

```sql
-- Restore with a snapshot ID
ALTER TABLE <table> FLASHBACK TO (SNAPSHOT => '<snapshot-id>');

-- Restore with a snapshot timestamp
ALTER TABLE <table> FLASHBACK TO (TIMESTAMP => '<timestamp>'::TIMESTAMP);
```

## Examples

```sql
CREATE TABLE mytable(a int);

INSERT INTO mytable VALUES(1);
INSERT INTO mytable VALUES(2);
INSERT INTO mytable VALUES(3);

SELECT * FROM mytable;

a|
-+
3|
2|
1|

-- Retrieve snapshot information
SELECT snapshot_id, timestamp FROM FUSE_SNAPSHOT('default','mytable');

snapshot_id                     |timestamp                    |
--------------------------------+-----------------------------+
2648bee0e85044f9879e71f1d37f453b|2022-12-06 20:50:53.324485000|
7e5d4f7ebdbc44d08116771193533346|2022-12-06 20:50:23.623331000|
41c94c7ea47a432388770ada0a66b6c0|2022-12-06 20:49:56.125918000|

-- Restore with a snapshot ID
ALTER TABLE mytable FLASHBACK TO (SNAPSHOT => '7e5d4f7ebdbc44d08116771193533346');

SELECT * FROM mytable;

a|
-+
2|
1|

-- Restore with a snapshot timestamp
ALTER TABLE mytable FLASHBACK TO (TIMESTAMP => '2022-12-06 20:49:56.125918000'::TIMESTAMP);

SELECT * FROM mytable;

a|
-+
1|
```