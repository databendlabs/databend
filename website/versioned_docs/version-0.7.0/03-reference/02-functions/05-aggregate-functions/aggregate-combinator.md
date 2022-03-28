---
title: Aggregate Function Combinators
---

# Aggregate Function Combinators

The name of an aggregate function can have a suffix appended to it. This changes the way the aggregate function works.


## Distinct

Every unique combination of arguments will be aggregated only once. Repeating values are ignored.

```
count(distinct(expression))
sum(distinct(expression))
min(distinct(expression))
max(distinct(expression))
```

## Examples

```sql
mysql> SELECT sum(distinct(number%3)) FROM numbers_mt(10);
+----------------------------+
| sum(distinct (number % 3)) |
+----------------------------+
|                          3 |
+----------------------------+
```



