---
title: NOT RLIKE
---

Returns 1 if the string expr doesn't match the regular expression specified by the pattern pat, 0 otherwise.

## Syntax

```sql
<expr> not rlike <pattern>
```

## Examples

```sql
SELECT 'databend' not rlike 'd*';
+-----------------------------+
| ('databend' not rlike 'd*') |
+-----------------------------+
|                           0 |
+-----------------------------+
```