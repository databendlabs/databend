---
title: SKEWNESS
---

Aggregate function.

The `SKEWNESS()` function returns the skewness of all input values.

## Syntax

```sql
SKEWNESS(expression)
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

select skewness(k), skewness(v), skewness(v2) from aggr;
+--------------------+----------------------+--------------------+
| skewness(k)        | skewness(v)          | skewness(v2)       |
+--------------------+----------------------+--------------------+
| -3.316624790355393 | -0.16344366935199225 | 0.3654008511025841 |
+--------------------+----------------------+--------------------+
```
