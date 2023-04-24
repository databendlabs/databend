---
id: string-length
title: LENGTH
---

Return the length of a string in bytes.

## Syntax

```sql
LENGTH(<str>)
```

## Arguments

| Arguments | Description |
|-----------|-------------|
| `<str>`   | The string. |

## Return Type

`BIGINT`

## Examples

```sql
SELECT LENGTH('Word');
+----------------+
| LENGTH('Word') |
+----------------+
|              4 |
+----------------+
```
