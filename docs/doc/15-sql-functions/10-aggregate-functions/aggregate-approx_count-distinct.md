---
title: APPROX_COUNT_DISTINCT
---

Estimates the number of distinct values in a data set with the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm. 

The HyperLogLog algorithm provides an approximation of the number of unique elements using little memory and time. Consider using this function when dealing with large data sets where an estimated result can be accepted. In exchange for some accuracy, this is a fast and efficient method of returning distinct counts.

To get an accurate result, use [COUNT_DISTINCT](aggregate-count-distinct.md). See [Examples](#examples) for more explanations.

## Syntax

```sql
APPROX_COUNT_DISTINCT(<expr>)
```

## Return Type

Integer.

## Examples

Both the APPROX_COUNT_DISTINCT and COUNT_DISTINCT functions answer the question “How Many Distinct”. APPROX_COUNT_DISTINCT computes the distinct counts using the HyperLogLog algorithm and returns an estimated result with less memory and time than COUNT_DISTINCT which returns the exact number.

These examples return the number of distinct visitors to the Databend website based on their IP address:

```sql
SELECT APPROX_COUNT_DISTINCT(ipaddress) FROM webvisitors;

---
3096


SELECT COUNT(DISTINCT ipaddress) FROM webvisitors;

---
3099
```

The results above are not the same because `3096` is an estimated value.