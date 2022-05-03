---
title: regexp
---

Returns 1 if the string expr matches the regular expression specified by the pattern pat, 0 otherwise.

## Syntax

```sql
expr REGEXP pat
```

## Examples

```sql
MySQL [(none)]> SELECT 'databend' REGEXP 'd*';
+--------------------------+
| ('databend' regexp 'd*') |
+--------------------------+
|                        1 |
+--------------------------+
```