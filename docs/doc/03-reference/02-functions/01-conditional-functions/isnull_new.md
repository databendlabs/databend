---
title: isnull
---

Checks whether a value is not NULL.

## Syntax

```sql
isNotNull(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | A value with non-compound data type. |



## Return Type

If x is NULL, ISNULL() returns true, otherwise it returns false.

## Examples

```sql
select isNull(3)
```
