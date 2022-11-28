---
title: REGEXP_LIKE
---

REGEXP_LIKE function is used to check that whether the string matches regular expression.

## Syntax

```sql
REGEXP_LIKE(expr, pat[, match_type])
```

## Arguments

| Arguments  | Description                                                                       |
| ---------- | --------------------------------------------------------------------------------- |
| expr       | The string expr that to be matched                                                |
| pat        | The regular expression                                                            |
| match_type | Optional. match_type argument is a string that specifying how to perform matching |

`match_type` may contain any or all the following characters:

* `c`: Case-sensitive matching.
* `i`: Case-insensitive matching.
* `m`: Multiple-line mode. Recognize line terminators within the string. The default behavior is to match line terminators only at the start and end of the string expression.
* `n`: The `.` character matches line terminators. The default is for `.` matching to stop at the end of a line.
* `u`: Unix-only line endings. Not be supported now.

## Return Type

A number data type value. Returns `1` if the string expr matches the regular expression specified by the pattern pat, `0` otherwise. If expr or pat is NULL, the return value is NULL.

## Examples

```sql
SELECT REGEXP_LIKE('a', '^[a-d]');
+----------------------------+
| REGEXP_LIKE('a', '^[a-d]') |
+----------------------------+
|                          1 |
+----------------------------+

SELECT REGEXP_LIKE('abc', 'ABC');
+---------------------------+
| REGEXP_LIKE('abc', 'ABC') |
+---------------------------+
|                         1 |
+---------------------------+

SELECT REGEXP_LIKE('abc', 'ABC', 'c');
+--------------------------------+
| REGEXP_LIKE('abc', 'ABC', 'c') |
+--------------------------------+
|                              0 |
+--------------------------------+

SELECT REGEXP_LIKE('new*\n*line', 'new\\*.\\*line');
+-------------------------------------------+
| REGEXP_LIKE('new*
*line', 'new\*.\*line') |
+-------------------------------------------+
|                                         0 |
+-------------------------------------------+

SELECT REGEXP_LIKE('new*\n*line', 'new\\*.\\*line', 'n');
+------------------------------------------------+
| REGEXP_LIKE('new*
*line', 'new\*.\*line', 'n') |
+------------------------------------------------+
|                                              1 |
+------------------------------------------------+
```
