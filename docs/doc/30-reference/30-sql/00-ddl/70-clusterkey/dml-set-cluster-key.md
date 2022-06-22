---
title: SET CLUSTER KEY
description:
  Sets a cluster key when creating a table.
---

Sets a cluster key when creating a table.

See also:

* [ALTER CLUSTER KEY](./dml-alter-cluster-key.md) 
* [DROP CLUSTER KEY](./dml-drop-cluster-key.md)

## Syntax

```sql
CREATE TABLE <name> ... CLUSTER BY ( <expr1> [ , <expr2> ... ] )
```

## Examples

This command creates a table clustered by columns:

```sql
CREATE TABLE t1(a int, b int) CLUSTER BY(b,a);
```