---
title: HUMANIZE_SIZE
---

Returns the readable size with a suffix(KB, MB, etc).

## Syntax

```sql
HUMANIZE_SIZE(x);
```

## Arguments

| Arguments | Description                |
|-----------|----------------------------|
| x         | The numerical size.        |


## Return Type

String.

## Examples

```sql
SELECT HUMANIZE_SIZE(1000 * 1000)
+-------------------------+
| HUMANIZE_SIZE((1000 * 1000)) |
+-------------------------+
| 1 MB                    |
+-------------------------+
```
