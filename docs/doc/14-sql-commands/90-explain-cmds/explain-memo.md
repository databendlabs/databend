---
title: EXPLAIN MEMO
---

Returns the internal structure `Memo` of the query.

## Syntax

```sql
EXPLAIN MEMO <query_statement>
```

## Examples

```sql
EXPLAIN MEMO SELECT * FROM numbers(10) t, numbers(100) t1;

 ----
 Group #0                            
 ├── best cost: [#1] 10              
 ├── LogicalGet []                   
 └── PhysicalScan []                 
                                     
 Group #1                            
 ├── best cost: [#1] 100             
 ├── LogicalGet []                   
 └── PhysicalScan []                 
                                     
 Group #2                            
 ├── best cost: [#3] 310             
 ├── LogicalJoin [#0, #1]       
 ├── LogicalJoin [#1, #0]       
 ├── PhysicalHashJoin [#0, #1]       
 └── PhysicalHashJoin [#1, #0]
```
