---
title: FIND_IN_SET
---

Returns a value in the range of 1 to N if the string str is in the string list strlist consisting of N substrings.
A string list is a string composed of substrings separated by , characters.
Returns 0 if str is not in strlist or if strlist is the empty string.
Returns NULL if either argument is NULL.
This function does not work properly if the first argument contains a comma (,) character.

## Syntax

```sql
FIND_IN_SET(str,strlist)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str | The string. |
| strlist | The string. |

## Return Type

A number data type value.

## Examples

```txt
SELECT FIND_IN_SET('b','a,b,c,d');
+-----------------------------+
| FIND_IN_SET('b', 'a,b,c,d') |
+-----------------------------+
|                           2 |
+-----------------------------+
```
