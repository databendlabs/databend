---
title: SET CLUSTER KEY
description:
  Sets a cluster key when creating a table.
---

Set a cluster key when creating a table.

Cluster key is intended to improve query performance by physically clustering data together. For example, when you set a column as your cluster key for a table, the table data will be physically sorted by the column you set. This will maximize the query performance if your most queries are filtered by the column.

> **Note:** For String column, the cluster statistics uses only the first 8 bytes. You can use a substring to provide sufficient cardinality.

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

CREATE TABLE t2(a int, b string) CLUSTER BY(SUBSTRING(b, 5, 6));
```