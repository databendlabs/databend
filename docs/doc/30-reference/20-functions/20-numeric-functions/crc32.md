---
title: CRC32
description: CRC32(x) function
---

Returns the CRC32 checksum of a string.

## Syntax

```sql
CRC32(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The argument is expected to be a string and (if possible) is treated as one if it is not. |

## Return Type

UInt32.

## Examples

```sql
SELECT CRC32('MySQL');
+----------------+
| CRC32('MySQL') |
+----------------+
|     3259397556 |
+----------------+

SELECT CRC32('mysql');
+----------------+
| CRC32('mysql') |
+----------------+
|     2501908538 |
+----------------+

SELECT CRC32(NULL);
+-------------+
| CRC32(NULL) |
+-------------+
|        NULL |
+-------------+

SELECT CRC32(12);
+------------+
| CRC32(12)  |
+------------+
| 1330857165 |
+------------+
```
