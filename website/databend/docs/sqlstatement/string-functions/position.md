---
title: POSITION
---

POSITION(substr IN str) is a synonym for LOCATE(substr,str).
Returns the position of the first occurrence of substring substr in string str.
Returns 0 if substr is not in str. Returns NULL if any argument is NULL.

## Syntax

```sql
POSITION(substr IN str)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| substr | The substring. |
| str | The string. |

## Return Type

A number data type value.

## Examples

```txt
SELECT POSITION('bar' IN 'foobarbar')
+----------------------------+
| POSITION('bar' IN 'foobarbar') |
+----------------------------+
|                          4 |
+----------------------------+

SELECT POSITION('xbar' IN 'foobar')
+--------------------------+
| POSITION('xbar' IN 'foobar') |
+--------------------------+
|                        0 |
+--------------------------+
```
