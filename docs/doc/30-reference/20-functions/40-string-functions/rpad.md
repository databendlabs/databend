---
title: RPAD
---

Returns the string str, right-padded with the string padstr to a length of len characters.
If str is longer than len, the return value is shortened to len characters.

## Syntax

```sql
RPAD(str,len,padstr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str | The string. |
| len | The length. |
| padstr | The pad string. |

## Return Type

A number data type value.

## Examples

```txt
SELECT RPAD('hi',5,'?');
+--------------------+
| RPAD('hi', 5, '?') |
+--------------------+
| hi???              |
+--------------------+

SELECT RPAD('hi',1,'?');
+--------------------+
| RPAD('hi', 1, '?') |
+--------------------+
| h                  |
+--------------------+
```
