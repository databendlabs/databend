---
title: ELT
---

Returns the Nth element of the list of strings: str1 if N = 1, str2 if N = 2, and so on.
Returns NULL if N is less than 1 or greater than the number of arguments.
ELT() is the complement of FIELD().

## Syntax

```sql
ELT(N,str1,str2,str3,...)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| N | The position. |
| strN | The string. |

## Return Type

A string data type value.

## Examples

```txt
SELECT ELT(1, 'Aa', 'Bb', 'Cc', 'Dd');
+--------------------------------+
| ELT(1, 'Aa', 'Bb', 'Cc', 'Dd') |
+--------------------------------+
| Aa                             |
+--------------------------------+

SELECT ELT(4, 'Aa', 'Bb', 'Cc', 'Dd');
+--------------------------------+
| ELT(4, 'Aa', 'Bb', 'Cc', 'Dd') |
+--------------------------------+
| Dd                             |
+--------------------------------+
```
