---
title: Arithmetic Operators
title_includes: plus, minus, multiply, div, divide, mod, caret, modulo, negate, +, -, /, //, %, *, ^, |/, ||/, @, &, |, #, ~, <<, >>
---

SQL arithmetic operators.

| Operator              | Description                                            | Example                    | Result |
|-----------------------|--------------------------------------------------------|----------------------------|--------|
| **+ (unary)**         | Returns `a`                                            | **+5**                     | 5      |
| **+**                 | Adds two numeric expressions                           | **4 + 1**                  | 5      |
| **- (unary)**         | Negates the numeric expression                         | **-5**                     | -5     |
| **-**                 | Subtract two numeric expressions                       | **4 - 1**                  | 3      |
| __*__                 | Multiplies two numeric expressions                     | **4 * 1**                  | 4      |
| **/**                 | Divides one numeric expression (`a`) by another (`b`)  | **4 / 2**                  | 2      |
| **//**                | Computes the integer division of numeric expression    | **4 // 3**                 | 1      |
| **%**                 | Computes the modulo of numeric expression              | **4 % 2**                  | 0      |
| **^**                 | Computes the exponentiation of numeric expression      | **4 ^ 2**                  | 16     |
| **&verbar;/**         | Computes the square root of numeric expression         | **&verbar;/ 25.0**         | 5      |
| **&verbar;&verbar;/** | Computes the cube root of numeric expression           | **&verbar;&verbar;/ 27.0** | 3      |
| **@**                 | Computes the abs of numeric expression                 | **@ -5.0**                 | 5      |
| **&**                 | Computes the bitwise and of numeric expression         | **91 & 15**                | 11     |
| **&verbar;**          | Computes the bitwise or of numeric expression          | **32 &verbar; 3**          | 35     |
| **#**                 | Computes the bitwise xor of numeric expression         | **17 # 5**                 | 20     |
| **~**                 | Computes the bitwise not of numeric expression         | **~ 1**                    | ~2     |
| **<<**                | Computes the bitwise shift left of numeric expression  | **1 << 4**                 | 16     |
| **>>**                | Computes the bitwise shift right of numeric expression | **8 >> 2**                 | 2      |
