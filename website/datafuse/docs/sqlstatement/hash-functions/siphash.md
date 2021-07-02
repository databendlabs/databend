---
id: hash-siphash
title: SIPHASH
---

Produces a 64-bit [SipHash](https://131002.net/siphash) hash value.

## Syntax

```sql
SIPHASH(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression. <br /> This may be a column name, the result of another function, or a math operation.

## Return Type

A UInt64 data type hash value.


## Examples

```
mysql> SELECT SIPHASH('1234567890');
+---------------------+
| SIPHASH(1234567890) |
+---------------------+
| 9027491583908826579 |
+---------------------+

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

mysql> SELECT SIPHASH(number) FROM numbers(2);
+----------------------+
| siphash(number)      |
+----------------------+
| 13646096770106105413 |
|  2206609067086327257 |
+----------------------+

```
