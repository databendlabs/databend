---
title: TO_NULLABLE
---

Converts the argument type to `Nullable`.


## Syntax

```sql
TO_NULLABLE(x);
```

## Arguments

| Arguments | Description                |
|-----------|----------------------------|
| x         | The original value.        |


## Return Type

Returns the original datatype from the `Nullable` type; Returns the wrapped `Nullable` datatype for non-`Nullable` type.

## Examples

```sql
select to_nullable(3);

+----------------+
| to_nullable(3) |
+----------------+
|              3 |
+----------------+
```

```sql
select to_nullable(null);

+-------------------+
| to_nullable(NULL) |
+-------------------+
|              NULL |
+-------------------+
```
 



 