---
title: Arithmetic Operators
title_includes: plus, minus, multiply, div, divide, mod, modulo, negate, +, -, /, %, *
---

Arithmetic Operators.

## Syntax

| Operator    | Syntax  | Description                                            |
| ----------- | ------- | ------------------------------------------------------ |
| `+` (unary) | `+a`    | Returns `a`.                                           |
| `+`         | `a + b` | Adds two numeric expressions.                          |
| `-` (unary) | `-a`    | Negates the numeric expression.                        |
| `-`         | `a - b` | Subtract two numeric expressions.                      |
| `*`         | `a * b` | Multiplies two numeric expressions.                    |
| `/`         | `a / b` | Divides one numeric expression (`a`) by another (`b`). |
| `%`         | `a % b` | Computes the modulo of numeric expression.             |


## Examples

```sql
SELECT +2;
+------+
| 2    |
+------+
|    2 |
+------+

SELECT 5+2;
+---------+
| (5 + 2) |
+---------+
|       7 |
+---------+

SELECT -2;
+------+
| -2   |
+------+
|   -2 |
+------+

SELECT 5-2;
+---------+
| (5 - 2) |
+---------+
|       3 |
+---------+

SELECT 5*2;
+---------+
| (5 * 2) |
+---------+
|      10 |
+---------+

SELECT 5/2;
+---------+
| (5 / 2) |
+---------+
|     2.5 |
+---------+

SELECT 5%2;
+---------+
| (5 % 2) |
+---------+
|       1 |
+---------+
```