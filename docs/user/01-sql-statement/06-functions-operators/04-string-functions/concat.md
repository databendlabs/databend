---
title: CONCAT
---

Returns the string that results from concatenating the arguments. May have one or more arguments. If all arguments are nonbinary strings, the result is a nonbinary string. If the arguments include any binary strings, the result is a binary string. A numeric argument is converted to its equivalent nonbinary string form.

## Syntax

```sql
CONCAT(column1, ...)
```

## Arguments

| Arguments   | Description   |
| ----------- | ------------- |
| column      | string column |

## Return Type

A String data type value Or Null data type.

## Examples

```txt
SELECT CONCAT('data', 'bend');
+------------------------+
| concat('data', 'bend') |
+------------------------+
| databend               |
+------------------------+

SELECT CONCAT('data', NULL, 'bend');
+------------------------------+
| CONCAT('data', NULL, 'bend') |
+------------------------------+
|                         NULL |
+------------------------------+

SELECT CONCAT(14.3);
+--------------+
| concat(14.3) |
+--------------+
| 14.3         |
+--------------+

SELECT CONCAT(number, 'a', number+1) FROM NUMBERS(3) ORDER BY number;
+-----------------------------------+
| CONCAT(number, 'a', (number + 1)) |
+-----------------------------------+
| 0a1                               |
| 1a2                               |
| 2a3                               |
+-----------------------------------+

SELECT CONCAT(number, NULL) from numbers(4);
+----------------------+
| CONCAT(number, NULL) |
+----------------------+
|                 NULL |
|                 NULL |
|                 NULL |
|                 NULL |
+----------------------+
```
