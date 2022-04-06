---
title: LOCATE
---

The first syntax returns the position of the first occurrence of substring substr in string str.
The second syntax returns the position of the first occurrence of substring substr in string str, starting at position pos.
Returns 0 if substr is not in str. Returns NULL if any argument is NULL.

## Syntax

```sql
LOCATE(substr,str)
LOCATE(substr,str,pos)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| substr | The substring. |
| str | The string. |
| str | The position. |

## Return Type

A number data type value.

## Examples

```txt
SELECT LOCATE('bar', 'foobarbar')
+----------------------------+
| LOCATE('bar', 'foobarbar') |
+----------------------------+
|                          4 |
+----------------------------+

SELECT LOCATE('xbar', 'foobar')
+--------------------------+
| LOCATE('xbar', 'foobar') |
+--------------------------+
|                        0 |
+--------------------------+

SELECT LOCATE('bar', 'foobarbar', 5)
+-------------------------------+
| LOCATE('bar', 'foobarbar', 5) |
+-------------------------------+
|                             7 |
+-------------------------------+
```
