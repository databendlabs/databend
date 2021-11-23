---
id: numeric-rand
title: PI
---

Returns a random floating-point value v in the range 0 <= v < 1.0.
To obtain a random integer R in the range i <= R < j, use the expression FLOOR(i + RAND() * (j − i)).

## Syntax

```sql
RAND()
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |

## Return Type

A Float64 data type value.

## Examples

```txt
mysql> SELECT RAND();
+---------------------+
| rand()              |
+---------------------+
| 0.07669816779607708 |
+---------------------+
```
