---
title: SHOW INDEXES
---

Shows the created aggregating indexes. Equivalent to `SELECT * FROM system.indexes`.

See also: [system.indexes](../../13-sql-reference/20-system-tables/system-indexes.md)

## Syntax

```sql
SHOW INDEXES;
```

## Example

```sql
CREATE TABLE t1(a int,b int);

CREATE AGGREGATING INDEX idx1 AS SELECT SUM(a), b FROM default.t1 WHERE b > 3 GROUP BY b；

SHOW INDEXES;

—───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  name  │     type    │                         definition                        │         created_on         │
│ String │    String   │                           String                          │          Timestamp         │
├────────┼─────────────┼───────────────────────────────────────────────────────────┼────────────────────────────┤
│ idx1   │ AGGREGATING │ SELECT SUM(a), b FROM default.t1 WHERE (b > 3) GROUP BY b │ 2023-05-20 02:41:50.143182 │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
