---
title: FIELD
---

Returns the index (position) of str in the str1, str2, str3, ... list. Returns 0 if str is not found.
If str is NULL, the return value is 0 because NULL fails equality comparison with any value.
FIELD() is the complement of ELT().

## Syntax

```sql
FIELD(str,str1,str2,str3,...)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| strN | The string. |

## Return Type

An numeric data type value.

## Examples

```txt
SELECT FIELD('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');
+-------------------------------------------+
| FIELD('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff') |
+-------------------------------------------+
|                                         2 |
+-------------------------------------------+
1 row in set (0.01 sec)

SELECT FIELD('Gg', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');
+-------------------------------------------+
| FIELD('Gg', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff') |
+-------------------------------------------+
|                                         0 |
+-------------------------------------------+
1 row in set (0.01 sec)
```
