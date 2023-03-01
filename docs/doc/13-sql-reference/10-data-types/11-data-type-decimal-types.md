---
title: Decimal
description:  Decimal types are high-precision numeric values to be stored and manipulated.
---

## Decimal Data Types

The Decimal type is useful for applications that require exact decimal representations, such as financial calculations or scientific computations.

We can use `DECIMAL(P, S)` to indicate decimal types. 

- `P` is the precision, which is the total number of digits in the number, it's range is [1, 76].
- `S` is the scale, which is the number of digits to the right of the decimal point, it's range is [0, P].

If `P` is less than 38, the physical datatype of decimal is `Decimal128`, otherwise it's `Decimal256`.

For a DECIMAL(P, S) data type:
* The minimum value is `-10^P + 1` divided by `10^S`.
* The maximum value is `10^P - 1` divided by `10^S`.
 
If you have a `DECIMAL(10, 2)` , you can store values with up to `10 digits`, with `2 digits` to the right of the decimal point. The minimum value is `-9999999.99`, and the maximum value is `9999999.99`.

## Example

```sql

-- Create a table with decimal data type.
create table decimal(value decimal(36, 18));

-- Insert two values.
insert into decimal values(0.152587668674722117), (0.017820781941443176);

select * from decimal;
+----------------------+
| value                |
+----------------------+
| 0.152587668674722117 |
| 0.017820781941443176 |
+----------------------+
```

## Precision Inference

DECIMAL has a set of complex rules for precision inference. Different rules will be applied for different expressions to infer the precision.

### Arithmetic Operations

- Addition/Subtraction: `DECIMAL(a, b) + DECIMAL(x, y) -> DECIMAL(max(a - b, x - y) + max(b, y) + 1, max(b, y))`, which means both integer and decimal parts use the larger value of the two operands.

- Multiplication: `DECIMAL(a, b) * DECIMAL(x, y) -> DECIMAL(a + x, b + y)`.

- Division: `DECIMAL(a, b) / DECIMAL(x, y) -> DECIMAL(a + y, b)`.

### Comparison Operations

- Decimal can be compared with other numeric types.
- Decimal can be compared with other decimal types.

### Aggregate Operations

- SUM: `SUM(DECIMAL(a, b)) -> DECIMAL(MAX, b)`
- AVG: `AVG(DECIMAL(a, b)) -> DECIMAL(MAX, max(b, 4))`

where `MAX` is 38 for decimal128 and 76 for decimal256.

## Adjusting Result Precision

Different users have different precision requirements for DECIMAL. The above rules are the default behavior of databend. If users have different precision requirements, they can adjust the precision in the following ways:

If the expected result precision is higher than the default precision, adjust the input precision to adjust the result precision. For example, if the user expects to compute AVG(col) to get DECIMAL(x, y) as the result, where col is of type DECIMAL(a, b), the expression can be rewritten as `cast(AVG(col) as Decimal(x, y)` or `AVG(col)::Decimal(x, y)`.

Note that in the conversion or calculation of decimal types, if the integer part overflows, an error will be thrown, and if the precision of the decimal part overflows, it will be directly discarded instead of being rounded.
