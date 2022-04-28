---
title: rlike
---

Returns 1 if the string expr matches the regular expression specified by the pattern pat, 0 otherwise.

## Syntax

```sql
expr rlike pat 
```

## Examples

```sql
MySQL [(none)]> SELECT 'databend' rlike 'd*';
+-------------------------+
| ('databend' rlike 'd*') |
+-------------------------+
|                       1 |
+-------------------------+
```