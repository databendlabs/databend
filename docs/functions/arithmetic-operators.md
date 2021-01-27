---
id: arithmetic-operators
title: Arithmetic Operators
---

Arithmetic Operators work for any pair of types from UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, or Float64.


## a + b operator, `add` function

Calculates the sum of the numbers.

```text
mysql> SELECT number+1, add(number, 1) FROM system.numbers(3);
+--------------+--------------+
| (number + 1) | (number + 1) |
+--------------+--------------+
|            1 |            1 |
|            2 |            2 |
|            3 |            3 |
+--------------+--------------+
3 rows in set (0.01 sec)
```

## a - b operator, `minus` function

```text
mysql> SELECT number-1, minus(number, 1) FROM system.numbers(3) WHERE number > 0;
+--------------+--------------+
| (number - 1) | (number - 1) |
+--------------+--------------+
|            0 |            0 |
|            1 |            1 |
+--------------+--------------+
2 rows in set (0.01 sec)
```

## a * b operator, `multiply` function

Calculates the product of the numbers.

```text
mysql> SELECT number * 3, multiply(number, 3) FROM system.numbers(3);
+--------------+--------------+
| (number * 3) | (number * 3) |
+--------------+--------------+
|            0 |            0 |
|            3 |            3 |
|            6 |            6 |
+--------------+--------------+
3 rows in set (0.00 sec)
```

## a / b operator, `divide` function

Calculates the quotient of the numbers.

```text
mysql> SELECT number /  3, divide(number, 3) FROM system.numbers(4);
+--------------------+--------------------+
| (number / 3)       | (number / 3)       |
+--------------------+--------------------+
|                  0 |                  0 |
| 0.3333333333333333 | 0.3333333333333333 |
| 0.6666666666666666 | 0.6666666666666666 |
|                  1 |                  1 |
+--------------------+--------------------+
4 rows in set (0.00 sec)
```
