---
title: HUMANIZE_SIZE
---

Returns the readable size with a suffix(KiB, MiB, etc).

## Syntax

```sql
HUMANIZE_SIZE(x);
```

## Arguments

| Arguments | Description         |
| --------- | ------------------- |
| x         | The numerical size. |


## Return Type

String.

## Examples

```sql
SELECT HUMANIZE_SIZE(1024 * 1024)
+-------------------------+
| HUMANIZE_SIZE((1024 * 1024)) |
+-------------------------+
| 1 MiB                    |
+-------------------------+
```
