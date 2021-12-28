---
title: XXHASH32
---

Calculates an xxHash32 32-bit hash value for the string.
The value is returned as a UInt32 or NULL if the argument was NULL.

## Syntax

```sql
xxhash32(expression)
```

## Arguments

| Arguments  | Description       |
| ---------- | ----------------- |
| expression | The string value. |

## Return Type

A UInt32 data type hash value.

## Examples

```sql
mysql> SELECT XXHASH32('1234567890');
+------------------------+
| XXHASH32('1234567890') |
+------------------------+
|             3896585587 |
+------------------------+
```
