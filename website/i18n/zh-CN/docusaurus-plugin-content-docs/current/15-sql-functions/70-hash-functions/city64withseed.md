---
title: City64WithSeed
---

Calculates a City64WithSeed 64-bit hash for the string.

## Syntax

```sql
City64WithSeed(expression1, expression2)
```

## Arguments

| Arguments  | Description              |
| ---------- | ------------------------ |
| expression | The string to be hashed. |
| expression | Seed.                    |

## Return Type

A String data type hash value.

## Examples

```sql
SELECT City64WithSeed('1234567890', 12);
+----------------------------------+
| City64WithSeed('1234567890', 12) |
+----------------------------------+
|             10660895976650300430 |
+----------------------------------+

SELECT City64WithSeed('1234567890', 12.12);
+-------------------------------------+
| City64WithSeed('1234567890', 12.12) |
+-------------------------------------+
|                10660895976650300430 |
+-------------------------------------+

```
