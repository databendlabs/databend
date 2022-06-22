---
title: COALESCE
---

Checks from left to right whether `NULL` arguments were passed and returns the first non-`NULL` argument.

 
## Syntax

```sql
COALESCE(x,...)
```

## Arguments

| Arguments | Description                |
|-----------|----------------------------|
| x         | The original value.        |


## Return Type

Returns the first non-`NULL` argument, returns `NULL` if all arguments are `NULL`.
## Examples

```sql
SELECT COALESCE(NULL, 1, 2);

+----------------------+
| COALESCE(NULL, 1, 2) |
+----------------------+
|                    1 |
+----------------------+
```
 



 