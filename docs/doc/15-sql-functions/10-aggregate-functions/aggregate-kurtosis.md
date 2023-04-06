---
title: KURTOSIS
---

Aggregate function.

The `KURTOSIS()` function returns the excess kurtosis of all input values.

## Syntax

```sql
KURTOSIS(expression)
```

## Arguments

| Arguments   | Description                     |
| ----------- | -----------                     |
| expression  | Any numerical expression        |

## Return Type

Nullable Float64.

## Examples

```sql
create table aggr(k int, v int, v2 int null);

insert into aggr values
    (1, 10, null),
    (2, 10, 11),
    (2, 10, 15),
    (2, 10, 18),
    (2, 20, 22),
    (2, 20, 25),
    (2, 25, null),
    (2, 30, 35),
    (2, 30, 40),
    (2, 30, 50),
    (2, 30, 51);

select kurtosis(k), kurtosis(v), kurtosis(v2) from aggr;
+-------------------+---------------------+---------------------+
| kurtosis(k)       | kurtosis(v)         | kurtosis(v2)        |
+-------------------+---------------------+---------------------+
| 10.99999999999836 | -1.9614277138467147 | -1.4451196915855287 |
+-------------------+---------------------+---------------------+
```
