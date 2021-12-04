---
title: TRUNCATE
---

Returns the number X, truncated to D decimal places.
If D is 0, the result has no decimal point or fractional part.
D can be negative to cause D digits left of the decimal point of the value X to become zero.
The maximum absolute value for D is 30; any digits in excess of 30 (or -30) are truncated.

## Syntax

```sql
TRUNCATE(X, D)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| X | The numerical value. |
| D | The numerical value. |

## Return Type

A f64 data type value.

## Examples

```text
SELECT TRUNCATE(1.223,1);
+--------------------+
| TRUNCATE(1.223, 1) |
+--------------------+
|                1.2 |
+--------------------+

SELECT TRUNCATE(1.999,1);
+--------------------+
| TRUNCATE(1.999, 1) |
+--------------------+
|                1.9 |
+--------------------+

SELECT TRUNCATE(1.999,0);
+--------------------+
| TRUNCATE(1.999, 0) |
+--------------------+
|                  1 |
+--------------------+

SELECT TRUNCATE(-1.999,1);
+------------------------+
| TRUNCATE((- 1.999), 1) |
+------------------------+
|                   -1.9 |
+------------------------+

SELECT TRUNCATE(122,-2);
+----------------------+
| TRUNCATE(122, (- 2)) |
+----------------------+
|                  100 |
+----------------------+

SELECT TRUNCATE(10.28*100,0);
+----------------------------+
| TRUNCATE((10.28 * 100), 0) |
+----------------------------+
|                       1028 |
+----------------------------+
```
