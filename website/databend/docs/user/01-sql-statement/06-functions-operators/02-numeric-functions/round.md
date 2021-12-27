---
title: ROUND
---

Rounds the argument X to D decimal places.
The rounding algorithm depends on the data type of X. D defaults to 0 if not specified.
D can be negative to cause D digits left of the decimal point of the value X to become zero.
The maximum absolute value for D is 30; any digits in excess of 30 (or -30) are truncated.

## Syntax

```sql
ROUND(X, D)
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
SELECT ROUND(-1.23);
+-----------------+
| ROUND((- 1.23)) |
+-----------------+
|              -1 |
+-----------------+

SELECT ROUND(-1.58);
+-----------------+
| ROUND((- 1.58)) |
+-----------------+
|              -2 |
+-----------------+

SELECT ROUND(1.58);
+-------------+
| ROUND(1.58) |
+-------------+
|           2 |
+-------------+

SELECT ROUND(1.298, 1);
+-----------------+
| ROUND(1.298, 1) |
+-----------------+
|             1.3 |
+-----------------+

SELECT ROUND(1.298, 0);
+-----------------+
| ROUND(1.298, 0) |
+-----------------+
|               1 |
+-----------------+

SELECT ROUND(23.298, -1);
+----------------------+
| ROUND(23.298, (- 1)) |
+----------------------+
|                   20 |
+----------------------+

SELECT ROUND(0.12345678901234567890123456789012345, 35);
+--------------------------------+
| ROUND(0.12345678901234568, 35) |
+--------------------------------+
|            0.12345678901234568 |
+--------------------------------+
```
