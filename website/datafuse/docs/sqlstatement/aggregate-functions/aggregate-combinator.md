---
id: aggregate-count-distinct
title: Aggregate Function Combinators
---

# Aggregate Function Combinators

The name of an aggregate function can have a suffix appended to it. This changes the way the aggregate function works.

## If 

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
countIf(cond)
sumIf(column, cond)
avgIf(x, cond)
minIf(x, cond)
maxIf(x, cond)
```

### Examples

```
mysql> SELECT countIf(number, number > 7) FROM numbers_mt(10);
+-------------------------------+
| countIf(number, (number > 7)) |
+-------------------------------+
|                             2 |
+-------------------------------+
```

## Distinct

Every unique combination of arguments will be aggregated only once. Repeating values are ignored.

```
count(distinct(expression))
sum(distinct(expression))
min(distinct(expression))
max(distinct(expression))
```

### Examples

```
mysql> SELECT sum(distinct(number%3)) FROM numbers_mt(10);
+----------------------------+
| sum(distinct (number % 3)) |
+----------------------------+
|                          3 |
+----------------------------+
```



