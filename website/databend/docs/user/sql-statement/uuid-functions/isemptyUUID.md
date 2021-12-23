---
title: isemptyUUID
---

Checks whether an UUID is empty.

## Syntax

```sql
isemptyUUID(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | A UUID. |

## Return Type

If x is empty, isemptyUUID() returns 1, otherwise it returns 0.

## Examples

```sql
mysql> select isemptyUUID(generateUUIDv4());
+-------------------------------+
| isemptyUUID(generateUUIDv4()) |
+-------------------------------+
|                             0 |
+-------------------------------+


mysql> select isemptyUUID(zeroUUID());
+-------------------------+
| isemptyUUID(zeroUUID()) |
+-------------------------+
|                       1 |
+-------------------------+
```
