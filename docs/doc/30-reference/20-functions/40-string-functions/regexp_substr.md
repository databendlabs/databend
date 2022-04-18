---
title: REGEXP_SUBSTR
---

Returns the substring of the string `expr` that matches the regular expression specified by the pattern `pat`, NULL if there is no match. If expr or pat is NULL, the return value is NULL.

## Syntax

```sql
REGEXP_SUBSTR(expr, pat[, pos[, occurrence[, match_type]]])
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr  |  The string expr that to be matched |
| pat   |  The regular expression |
| pos   |  Optional. The position in expr at which to start the search. If omitted, the default is 1. |
| occurrence   |  Optional. Which occurrence of a match to search for. If omitted, the default is 1. |
| match_type  |  Optional. A string that specifies how to perform matching. The meaning is as described for REGEXP_LIKE(). |

## Return Type

A String data type value.

## Examples

```sql
SELECT REGEXP_SUBSTR('abc def ghi', '[a-z]+');
+----------------------------------------+
| REGEXP_SUBSTR('abc def ghi', '[a-z]+') |
+----------------------------------------+
| abc                                    |
+----------------------------------------+

SELECT REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1, 3);
+----------------------------------------------+
| REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1, 3) |
+----------------------------------------------+
| ghi                                          |
+----------------------------------------------+

SELECT REGEXP_SUBSTR('周 周周 周周周 周周周周', '周+', 2, 3);
+------------------------------------------------------------------+
| REGEXP_SUBSTR('周 周周 周周周 周周周周', '周+', 2, 3)            |
+------------------------------------------------------------------+
| 周周周周                                                         |
+------------------------------------------------------------------+

```
