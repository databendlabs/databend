---
id: string-char
title: CHAR
---

Return the character for each integer passed.

CHAR() interprets each argument N as an integer and returns a string consisting of the characters given by the code values of those integers. NULL values are skipped.

CHAR() arguments larger than 255 are converted into multiple result bytes.

## Syntax

```sql
CONCAT(N, ...)
```

## Arguments

| Arguments | Description |
|-----------|-------------|
| N         | Data Column |

## Return Type

A String data type value.

## Examples

```txt
SELECT CHAR(77,121,83,81,'76');
+-------------------------+
| CHAR(77,121,83,81,'76') |
+-------------------------+
| MySQL                   |
+-------------------------+
```
