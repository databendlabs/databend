---
title: ALTER CLUSTER KEY
description:
  Changes the cluster key for a table.
---

Changes the cluster key for a table.

See also:
[DROP CLUSTER KEY](./dml-drop-cluster-key.md)

## Syntax

```sql
ALTER TABLE <name> CLUSTER BY ( <expr1> [ , <expr2> ... ] )
```

## Examples

```sql
-- Create table
CREATE TABLE IF NOT EXISTS playground(a int, b int);

-- Add cluster key by columns
ALTER TABLE playground CLUSTER BY(b,a);

INSERT INTO playground VALUES(0,3),(1,1);
INSERT INTO playground VALUES(1,3),(2,1);
INSERT INTO playground VALUES(4,4);

SELECT * FROM playground ORDER BY b,a;
SELECT * FROM clustering_information('db1','playground');

-- Delete cluster key
ALTER TABLE playground DROP CLUSTER KEY;

-- Add cluster key by expressions
ALTER TABLE playground CLUSTER BY(rand()+a); 
```