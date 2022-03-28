---
title: INSTR
---

Returns the position of the first occurrence of substring substr in string str.
This is the same as the two-argument form of LOCATE(), except that the order of the arguments is reversed.

## Syntax

```sql
INSTR(str,substr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str | The string. |
| substr | The substring. |

## Return Type

A number data type value.

## Examples

```txt
SELECT INSTR('foobarbar', 'bar');
+---------------------------+
| INSTR('foobarbar', 'bar') |
+---------------------------+
|                         4 |
+---------------------------+

SELECT INSTR('xbar', 'foobar');
+-------------------------+
| INSTR('xbar', 'foobar') |
+-------------------------+
|                       0 |
+-------------------------+
```
