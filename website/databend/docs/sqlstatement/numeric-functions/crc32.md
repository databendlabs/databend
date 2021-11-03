---
id: numeric-crc32
title: CRC32
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

```
mysql> SELECT CRC32('MySQL');
+----------------+
| CRC32('MySQL') |
+----------------+
|     3259397556 |
+----------------+
1 row in set (0.09 sec)

mysql> SELECT CRC32('mysql');
+----------------+
| CRC32('mysql') |
+----------------+
|     2501908538 |
+----------------+
1 row in set (0.01 sec)

mysql> SELECT CRC32(NULL);
+-------------+
| CRC32(NULL) |
+-------------+
|        NULL |
+-------------+
1 row in set (0.00 sec)

mysql> SELECT CRC32(12);
+------------+
| CRC32(12)  |
+------------+
| 1330857165 |
+------------+
1 row in set (0.01 sec)
```
