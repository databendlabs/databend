---
title: LPAD
---

Returns the string str, left-padded with the string padstr to a length of len characters.
If str is longer than len, the return value is shortened to len characters.

## Syntax

```sql
LPAD(str,len,padstr)
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
SELECT LPAD('hi',4,'??');
+---------------------+
| LPAD('hi', 4, '??') |
+---------------------+
| ??hi                |
+---------------------+

SELECT LPAD('hi',1,'??');
+---------------------+
| LPAD('hi', 1, '??') |
+---------------------+
| h                   |
+---------------------+
```
