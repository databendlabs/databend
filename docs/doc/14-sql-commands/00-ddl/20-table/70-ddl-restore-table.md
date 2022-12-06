---
title: RESTORE TABLE
---

Restores a table to an earlier version with a snapshot ID or timestamp.

The capability to restore a table is part of Databend's time travel feature. By specifying a snapshot ID or a timestamp of a table, you can restore the table to a prior state where the snapshot or timestamp was created. To retrieve the snapshots and timestamps of a table, use [FUSE_SNAPSHOT](../../../15-sql-functions/111-system-functions/fuse_snapshot.md).

## Syntax

```sql
-- Restore with a snapshot ID
ALTER TABLE <table> FLASHBACK TO (SNAPSHOT => '<snapshot-id>');

-- Restore with a timestamp
ALTER TABLE <table> FLASHBACK TO (TIMESTAMP => '<timestamp>'::TIMESTAMP);
```

## Examples

```sql

```
