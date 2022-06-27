---
title: DROP CLUSTER KEY
description:
  Deletes the cluster key for a table.
---

Deletes the cluster key for a table.

See also:
[ALTER CLUSTER KEY](./dml-alter-cluster-key.md) 

## Syntax

```sql
ALTER TABLE <name> DROP CLUSTER KEY
```

## Examples

This command drops the cluster key for table *test*:

```sql
ALTER TABLE test DROP CLUSTER KEY
```