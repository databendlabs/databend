---
title: ARRAY_AGG
title_includes: LIST
---

Aggregate function.

The `ARRAY_AGG()` function converts all the values of a column to an Array.

:::tip
The `LIST` function is alias to `ARRAY_AGG`.
:::

## Syntax

```sql
ARRAY_AGG(expression)
LIST(expression)
```

## Arguments

| Arguments   | Description    |
| ----------- | -------------- |
| expression  | Any expression |

## Return Type

the Array type that use the type of the value as inner type.

## Examples

```sql
SELECT ARRAY_AGG(number) FROM numbers(10);
+-----------------------+
| array_agg(number)     |
+-----------------------+
| [0,1,2,3,4,5,6,7,8,9] |
+-----------------------+
```
