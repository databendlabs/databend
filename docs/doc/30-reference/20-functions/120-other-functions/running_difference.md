---
title: RUNNING_DIFFERENCE
---

Calculates the difference between successive row values ​​in the data block.
Returns 0 for the first row and the difference from the previous row for each subsequent row.

## Syntax

```sql
RUNNING_DIFFERENCE( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>`| Any expression which generates numeric result, including integer numbers, real numbers, date  and datetime.   

## Return Type

Numeric Type

## Examples

```
databend :) DESC runing_difference_test;
┌─Field─┬─Type──┬─Null─┐
│ a     │ UInt8 │ NO   │
└───────┴───────┴──────┘

databend :) SELECT * FROM runing_difference_test;
┌──a─┐
│  1 │
│  3 │
│  5 │
│ 10 │
└────┘
┌──a─┐
│ 15 │
│ 20 │
└────┘

databend :) SELECT running_difference(a) FROM runing_difference_test;
┌─running_difference(a)┐
│                    0 │
│                    2 │
│                    2 │
│                    5 │
└──────────────────────┘
┌─running_difference(a)┐
│                    0 │
│                    5 │
└──────────────────────┘
```
