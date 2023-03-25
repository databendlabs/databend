---
title: LIST
---

Aggregate function.

The LIST() function converts all the values of a column to an Array.

## Syntax

```sql
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
SELECT LIST(number) FROM numbers(10);
+-----------------------+
| list(number)          |
+-----------------------+
| [0,1,2,3,4,5,6,7,8,9] |
+-----------------------+
```
