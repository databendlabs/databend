---
title: isnotemptyUUID
---

Checks whether an UUID is not empty.

## Syntax

```sql
isnotemptyUUID(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | A UUID. |

## Return Type

If x is not empty, isnotemptyUUID() returns 1, otherwise it returns 0.

## Examples

```sql
mysql> select isnotemptyUUID(generateUUIDv4());
+----------------------------------+
| isnotemptyUUID(generateUUIDv4()) |
+----------------------------------+
|                                1 |
+----------------------------------+


mysql> select isnotemptyUUID(zeroUUID());
+----------------------------+
| isnotemptyUUID(zeroUUID()) |
+----------------------------+
|                          0 |
+----------------------------+
```
