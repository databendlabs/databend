---
title: REGEXP_REPLACE
---

Replaces occurrences in the string `expr` that match the regular expression specified by the pattern `pat` with the replacement string `repl`, and returns the resulting string. If `expr`, `pat`, or `repl` is NULL, the return value is NULL.

## Syntax

```sql
REGEXP_REPLACE(expr, pat, repl[, pos[, occurrence[, match_type]]])
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr  |  The string expr that to be matched |
| pat   |  The regular expression |
| repl   |  The replacement string |
| pos   |  Optional. The position in expr at which to start the search. If omitted, the default is 1. |
| occurrence   |  Optional. Which occurrence of a match to replace. If omitted, the default is 0 (which means "replace all occurrences"). |
| match_type  |  Optional. A string that specifies how to perform matching. The meaning is as described for REGEXP_LIKE(). |

## Return Type

A String data type value.

## Examples

```sql
SELECT REGEXP_REPLACE('a b c', 'b', 'X');
+-----------------------------------+
| REGEXP_REPLACE('a b c', 'b', 'X') |
+-----------------------------------+
| a X c                             |
+-----------------------------------+

SELECT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3);
+----------------------------------------------------+
| REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3) |
+----------------------------------------------------+
| abc def X                                          |
+----------------------------------------------------+

SELECT REGEXP_REPLACE('周 周周 周周周', '周+', 'X', 3, 2);
+-----------------------------------------------------------+
| REGEXP_REPLACE('周 周周 周周周', '周+', 'X', 3, 2)        |
+-----------------------------------------------------------+
| 周 周周 X                                                 |
+-----------------------------------------------------------+
```
