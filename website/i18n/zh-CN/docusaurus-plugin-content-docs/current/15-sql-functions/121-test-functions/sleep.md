---
title: SLEEP
---

Sleeps `seconds` seconds on each data block.

!!! warning 
    Only used for testing where sleep is required.


## Syntax

```sql
SLEEP(seconds)
```

## Arguments

| Arguments | Description                                                    |
| --------- | -------------------------------------------------------------- |
| seconds   | Must be a constant column of any nonnegative number or float.｜ |

## Return Type

UInt8

## Examples

```sql
SELECT sleep(2);
+----------+
| sleep(2) |
+----------+
|        0 |
+----------+
```
