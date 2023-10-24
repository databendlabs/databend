---
title: TO_NULLABLE
---

Converts a value to its nullable equivalent.

When you apply this function to a value, it checks if the value is already able to hold NULL values or not. If the value is already able to hold NULL values, the function will return the value without making any changes.

However, if the value is not able to hold NULL values, the TO_NULLABLE function will modify the value to make it able to hold NULL values. It does this by wrapping the value in a structure that can hold NULL values, which means the value can now hold NULL values in the future.

## Syntax

```sql
TO_NULLABLE(x);
```

## Arguments

| Arguments | Description                |
|-----------|----------------------------|
| x         | The original value.        |


## Return Type

Returns a value of the same data type as the input value, but wrapped in a nullable container if the input value is not already nullable.

## Examples

```sql
SELECT typeof(3), TO_NULLABLE(3), typeof(TO_NULLABLE(3));

typeof(3)       |to_nullable(3)|typeof(to_nullable(3))|
----------------+--------------+----------------------+
TINYINT UNSIGNED|             3|TINYINT UNSIGNED NULL |

```