---
title: REGEXP_INSTR
---

Returns the starting index of the substring of the string `expr` that matches the regular expression specified by the pattern `pat`, `0` if there is no match. If `expr` or `pat` is NULL, the return value is NULL. Character indexes begin at `1`.

## Syntax

```sql
REGEXP_INSTR(expr, pat[, pos[, occurrence[, return_option[, match_type]]]])
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr  |  The string expr that to be matched |
| pat   |  The regular expression |
| pos   |  Optional. The position in expr at which to start the search. If omitted, the default is 1. |
| occurrence   |  Optional. Which occurrence of a match to search for. If omitted, the default is 1. |
| return_option   | Optional. Which type of position to return. If this value is 0, REGEXP_INSTR() returns the position of the matched substring's first character. If this value is 1, REGEXP_INSTR() returns the position following the matched substring. If omitted, the default is 0. |
| match_type  |  Optional. A string that specifies how to perform matching. The meaning is as described for REGEXP_LIKE(). |

## Return Type

A number data type value.

## Examples

```sql
SELECT REGEXP_INSTR('dog cat dog', 'dog');
+------------------------------------+
| REGEXP_INSTR('dog cat dog', 'dog') |
+------------------------------------+
|                                  1 |
+------------------------------------+

SELECT REGEXP_INSTR('dog cat dog', 'dog', 2);
+---------------------------------------+
| REGEXP_INSTR('dog cat dog', 'dog', 2) |
+---------------------------------------+
|                                     9 |
+---------------------------------------+

SELECT REGEXP_INSTR('aa aaa aaaa', 'a{2}');
+-------------------------------------+
| REGEXP_INSTR('aa aaa aaaa', 'a{2}') |
+-------------------------------------+
|                                   1 |
+-------------------------------------+

SELECT REGEXP_INSTR('aa aaa aaaa', 'a{4}');
+-------------------------------------+
| REGEXP_INSTR('aa aaa aaaa', 'a{4}') |
+-------------------------------------+
|                                   8 |
+-------------------------------------+
```
