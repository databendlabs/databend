---
title: system.indexes
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.50"/>

Contains information about the created aggregating indexes.

```sql
CREATE TABLE t1(a int,b int);

CREATE AGGREGATING INDEX idx1 AS SELECT SUM(a), b FROM default.t1 WHERE b > 3 GROUP BY b；

SELECT * FROM system.indexes；

—───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  name  │     type    │                         definition                        │         created_on         │
│ String │    String   │                           String                          │          Timestamp         │
├────────┼─────────────┼───────────────────────────────────────────────────────────┼────────────────────────────┤
│ idx1   │ AGGREGATING │ SELECT SUM(a), b FROM default.t1 WHERE (b > 3) GROUP BY b │ 2023-05-20 02:41:50.143182 │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

See also: [SHOW INDEXES](../../14-sql-commands/40-show/show-indexes.md)