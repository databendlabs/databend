---
title: isnotnull
---

Checks whether a value is NULL.

## Syntax

```sql
isNull(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | A value with non-compound data type. |



## Return Type

If x is NULL, returns false, otherwise it returns true.

## Examples

```sql
select isNotNull(3)
```
