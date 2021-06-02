---
id: hash-siphash
title: SIPHASH
---

Produces a 64-bit [SipHash](https://131002.net/siphash) hash value.

## Syntax

```sql
SIPHASH(<expr>)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | All DataFuse supported data types |

## Return Type

A UInt64 data type hash value.


## Examples

```
mysql> SELECT SIPHASH('1234567890');
+----------------------+
| SIPHASH(1234567890)  |
+----------------------+
| 18110648197875983073 |
+----------------------+

mysql> SELECT SIPHASH(1);
+---------------------+
| SIPHASH(1)          |
+---------------------+
| 2206609067086327257 |
+---------------------+

mysql> SELECT SIPHASH(1.2);
+---------------------+
| SIPHASH(1.2)        |
+---------------------+
| 2854037594257667269 |
+---------------------+

```
