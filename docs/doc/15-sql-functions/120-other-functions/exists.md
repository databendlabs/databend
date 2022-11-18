---
title: EXISTS
---

The exists condition is used in combination with a subquery and is considered "to be met" if the subquery returns at least one row.

## Syntax

```sql
WHERE EXISTS ( <subquery> );
```

## Examples
```sql
SELECT number FROM numbers(5) AS A WHERE exists (SELECT * FROM numbers(3) WHERE number=1); 
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
|      3 |
|      4 |
+--------+
```
