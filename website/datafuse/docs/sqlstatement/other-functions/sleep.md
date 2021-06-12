---
id: sleep
title: Sleep
---

Sleeps 'seconds' seconds on each data block.

## Syntax

```sql
sleep(seconds)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| seconds  | Must be a constant column of any nonnegative number or float.｜

## Return Type

UInt8

## Examples

```
mysql> SELECT sleep(2);
+----------+
| sleep(2) |
+----------+
|        0 |
+----------+
1 row in set (2.01 sec)

mysql> SELECT sleep(2.7);
+------------+
| sleep(2.7) |
+------------+
|          0 |
+------------+
1 row in set (2.71 sec)
```
