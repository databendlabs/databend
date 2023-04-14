---
title: Comparison Operators
title_includes: =, >=, >, !=, <=, <, <>
---

SQL Comparison Operators.

| Operator  | Description                     | Example       | Result |
|-----------|---------------------------------|---------------|--------|
| **=**     | a is equal to b                 | **2 = 2**     | TRUE   |
| **!=**    | a is not equal to b             | **2 != 3**    | TRUE   |
| **<\>**   | a is not equal to b             | **2 <\> 2**   | FALSE  |
| **>**     | a is greater than b             | **2 > 3**     | FALSE  |
| **>=**    | a is greater than or equal to b | **4 >= NULL** | NULL   |
| **<**     | a is less than b                | **2 < 3**     | TRUE   |
| **<=**    | a is less than or equal to b    | **2 <= 3**    | TRUE   |

## IS (NOT) NULL

| Operator                   | Description                                 | Example                    | Result |
|----------------------------|---------------------------------------------|----------------------------|--------|
| **expression IS NULL**     | TRUE if expression is NULL, FALSE otherwise | **(4>= NULL) IS NULL**     | TRUE   |
| **expression IS NOT NULL** | FALSE if expression is NULL, TRUE otherwise | **(4>= NULL) IS NOT NULL** | FALSE  |
