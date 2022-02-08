---
id: string-char
title: CHAR
---

Return the character for each integer passed.

## Syntax

```sql
CHAR(N, ...)
```

## Arguments

| Arguments | Description    |
|-----------|----------------|
| N         | Numeric Column |

## Return Type

A String data type value.

## Examples

```txt
SELECT CHAR(77,121,83,81,'76');
+-------------------------+
| CHAR(77,121,83,81,76) |
+-------------------------+
| MySQL                   |
+-------------------------+
```
