---
title: runningDifference
---

Calculates the difference between successive row values ​​in the data block.
Returns 0 for the first row and the difference from the previous row for each subsequent row.

## Syntax

```sql
runningDifference(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression which generates numberic result, including integer numbers, real numbers, date  and datetime.   

## Return Type

Numberic Type

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

databend :) SELECT runningDifference(a) FROM runing_difference_test;
┌─runningDifference(a)─┐
│                    0 │
│                    2 │
│                    2 │
│                    5 │
└──────────────────────┘
┌─runningDifference(a)─┐
│                    0 │
│                    5 │
└──────────────────────┘
```
