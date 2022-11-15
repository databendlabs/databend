---
title: EXPLAIN RAW
---

Shows the logical execution plan of a SQL statement that you can use to analyze, troubleshoot, and improve the efficiency of your queries.

## Syntax

```sql
EXPLAIN RAW <statement>
```

## Examples

```sql
explain raw select * from t1, t2 where (t1.a = t2.a and t1.a > 3) or (t1.a = t2.a);

Project: [a (#0),b (#1),a (#2),b (#3)]
 └── EvalScalar: [t1.a (#0), t1.b (#1), t2.a (#2), t2.b (#3)]
     └── Filter: [((t1.a (#0) = t2.a (#2)) AND (t1.a (#0) > 3)) OR (t1.a (#0) = t2.a (#2))]
         └── LogicalInnerJoin: equi-conditions: [], non-equi-conditions: []
             ├── LogicalGet: default.default.t1
             └── LogicalGet: default.default.t2

explain raw select * from t1 inner join t2 on t1.a = t2.a and t1.b = t2.b and t1.a > 2;

 ----
 Project: [a (#0),b (#1),a (#2),b (#3)]
 └── EvalScalar: [t1.a (#0), t1.b (#1), t2.a (#2), t2.b (#3)]
     └── LogicalInnerJoin: equi-conditions: [(t1.a (#0) = t2.a (#2)) AND (t1.b (#1) = t2.b (#3))], non-equi-conditions: [t1.a (#0) > 2]
         ├── LogicalGet: default.default.t1
         └── LogicalGet: default.default.t2
```