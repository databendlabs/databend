---
title: BLAKE3
---

Calculates an BLAKE3 256-bit checksum for the string.
The value is returned as a string of 64 hexadecimal digits or NULL if the argument was NULL.

## Syntax

```sql
blake3(expression)
```

## Arguments

| Arguments  | Description       |
| ---------- | ----------------- |
| expression | The string value. |

## Return Type

A String data type hash value.

## Examples

```text
mysql> SELECT BLAKE3('1234567890');
+------------------------------------------------------------------+
| BLAKE3('1234567890')                                             |
+------------------------------------------------------------------+
| d12e417e04494572b561ba2c12c3d7f9e5107c4747e27b9a8a54f8480c63e841 |
+------------------------------------------------------------------+
```
