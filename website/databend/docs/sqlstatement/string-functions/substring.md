---
title: SUBSTRING
---

SUBSTRING function is used to extract a string containing a specific number of characters from a particular position of a given string.
The forms without a len argument return a substring from string str starting at position pos.
The forms with a len argument return a substring len characters long from string str, starting at position pos.
It is also possible to use a negative value for pos.
In this case, the beginning of the substring is pos characters from the end of the string, rather than the beginning.
A negative value may be used for pos in any of the forms of this function. A value of 0 for pos returns an empty string.
The position of the first character in the string from which the substring is to be extracted is reckoned as 1.

## Syntax

```sql
SUBSTRING(str,pos);
SUBSTRING(str FROM pos);
SUBSTRING(str,pos,len);
SUBSTRING(str FROM pos FOR len)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str | The main string from where the character to be extracted |
| pos | The one-indexed position expression to start at. If negative, counts from the end |
| len | The number expression of characters to extract |

## Return Type

String

:::note
In SUBSTRING, the starting index point of a string is 1 (not 0).

In the following example, the starting index 3 represents the third character in the string, because the index starts from 1.
```
mysql> SELECT SUBSTRING('1234567890' FROM 3 FOR 3);
+---------------------------+
| SUBSTRING(1234567890,3,3) |
+---------------------------+
| 345                       |
+---------------------------+
```
:::

## Examples

```txt
SELECT SUBSTRING('Quadratically',5)
+-------------------------------+
| substring('Quadratically', 5) |
+-------------------------------+
| ratically                     |
+-------------------------------+

SELECT SUBSTRING('foobarbar' FROM 4);
+---------------------------+
| substring('foobarbar', 4) |
+---------------------------+
| barbar                    |
+---------------------------+

SELECT SUBSTRING('Quadratically',5,6);
+----------------------------------+
| substring('Quadratically', 5, 6) |
+----------------------------------+
| ratica                           |
+----------------------------------+

SELECT SUBSTRING('Sakila', -3);
+----------------------------+
| substring('Sakila', (- 3)) |
+----------------------------+
| ila                        |
+----------------------------+

SELECT SUBSTRING('Sakila', -5, 3);
+-------------------------------+
| substring('Sakila', (- 5), 3) |
+-------------------------------+
| aki                           |
+-------------------------------+

SELECT SUBSTRING('Sakila' FROM -4 FOR 2);
+-------------------------------+
| substring('Sakila', (- 4), 2) |
+-------------------------------+
| ki                            |
+-------------------------------+
```
