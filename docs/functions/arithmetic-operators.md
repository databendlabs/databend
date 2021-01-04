---
id: arithmetic-operators
title: Arithmetic Operators
---

Arithmetic Operators work for any pair of types from UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, or Float64.


## a + b operator

Calculates the sum of the numbers.

```
mysql> SELECT number+1 FROM system.numbers(3);
+--------------+
| (number + 1) |
+--------------+
|            1 |
|            2 |
|            3 |
+--------------+
3 rows in set (0.00 sec)
```

## a - b operator

```
mysql> SELECT number-1 FROM system.numbers(3) WHERE number > 0;
+--------------+
| (number - 1) |
+--------------+
|            0 |
|            1 |
+--------------+
2 rows in set (0.00 sec)
```

## a * b operator

Calculates the product of the numbers.

```
mysql> SELECT number*2 FROM system.numbers(3);
+--------------+
| (number * 2) |
+--------------+
|            0 |
|            2 |
|            4 |
+--------------+
3 rows in set (0.01 sec)
```

## a / b operator

Calculates the quotient of the numbers.

```
mysql> SELECT number/2 FROM system.numbers(3);
+--------------+
| (number / 2) |
+--------------+
|            0 |
|            0 |
|            1 |
+--------------+
3 rows in set (0.01 sec)
```






