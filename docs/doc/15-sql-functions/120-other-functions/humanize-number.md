---
title: HUMANIZE_NUMBER
---

Returns a readable number.

## Syntax

```sql
HUMANIZE_NUMBER(x);
```

## Arguments

| Arguments | Description                |
|-----------|----------------------------|
| x         | The numerical size.        |


## Return Type

String.

## Examples

```sql
SELECT HUMANIZE_NUMBER(1000 * 1000)
+-------------------------+
| HUMANIZE_NUMBER((1000 * 1000)) |
+-------------------------+
| 1 million               |
+-------------------------+
```
