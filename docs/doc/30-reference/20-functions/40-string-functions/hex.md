---
title: HEX
---

For a string argument str, HEX() returns a hexadecimal string representation of str where each byte of each character in str is converted to two hexadecimal digits. The inverse of this operation is performed by the UNHEX() function.

For a numeric argument N, HEX() returns a hexadecimal string representation of the value of N treated as a longlong (BIGINT) number. 

## Syntax

```sql
HEX(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr        | The number or string. |

## Examples

```sql
SELECT HEX('abc');
+------------+
| HEX('abc') |
+------------+
| 616263     |
+------------+

SELECT HEX(255);
+----------+
| HEX(255) |
+----------+
| ff       |
+----------+
```
