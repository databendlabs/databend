---
title: HUMANIZE
---

Returns the readable size with a suffix(KB, MB, etc).

## Syntax

```sql
HUMANIZE(x);
```

## Arguments

| Arguments | Description                |
|-----------|----------------------------|
| x         | The numerical size.        |


## Return Type

String.

## Examples

```sql
SELECT HUMANIZE(1000 * 1000)
+-------------------------+
| HUMANIZE((1000 * 1000)) |
+-------------------------+
| 1 MB                    |
+-------------------------+
```
