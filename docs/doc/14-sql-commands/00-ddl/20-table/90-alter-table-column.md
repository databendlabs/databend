---
title: ALTER Table Column
description:
  Add or drop column of a table.
---

Add or drop column of a table.

## Syntax

```sql
ALTER TABLE [IF EXISTS] <name> ADD COLUMN <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }]
ALTER TABLE [IF EXISTS] <name> DROP COLUMN <column_name>
```

## Examples

```sql
-- Create table
CREATE TABLE t(a int, b float);
INSERT INTO t VALUES(1,2);

-- Add a column
ALTER TABLE t ADD COLUMN c float DEFAULT 10;
-- Should return `1 2 10.0`
SELECT * FROM t;

-- Drop a column
ALTER TABLE t DROP COLUMN b;
-- Should return `1 10.0`
SELECT * FROM t;

```