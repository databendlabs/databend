---
title: XXHASH64
---

Calculates an xxHash64 64-bit hash value for the string.
The value is returned as a UInt64 or NULL if the argument was NULL.

## Syntax

```sql
xxhash64(expression)
```

## Arguments

| Arguments  | Description       |
| ---------- | ----------------- |
| expression | The string value. |

## Return Type

A UInt64 data type hash value.

## Examples

```sql
mysql> SELECT XXHASH64('1234567890');
+------------------------+
| XXHASH64('1234567890') |
+------------------------+
|   12237639266330420150 |
+------------------------+
```
