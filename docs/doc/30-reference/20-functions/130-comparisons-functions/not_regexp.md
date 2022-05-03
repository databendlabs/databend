---
title: not regexp
---

Returns 1 if the string expr doesn't match the regular expression specified by the pattern pat, 0 otherwise.

## Syntax

```sql
expr NOT REGEXP pat
```

## Examples

```sql
MySQL [(none)]> SELECT 'databend' NOT REGEXP 'd*';
+------------------------------+
| ('databend' not regexp 'd*') |
+------------------------------+
|                            0 |
+------------------------------+
```