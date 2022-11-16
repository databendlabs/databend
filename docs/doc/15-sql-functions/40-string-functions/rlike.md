---
title: RLIKE
---

Returns 1 if the string expr matches the regular expression specified by the pattern pat, 0 otherwise.

## Syntax

```sql
<expr> RLIKE <pattern>
```

## Examples

```sql
SELECT 'databend' rlike 'd*';
+-------------------------+
| ('databend' rlike 'd*') |
+-------------------------+
|                       1 |
+-------------------------+
```