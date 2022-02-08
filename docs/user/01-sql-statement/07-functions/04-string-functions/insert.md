---
title: INSERT
---

Returns the string str, with the substring beginning at position pos and len characters long replaced by the string newstr. Returns the original string if pos is not within the length of the string. Replaces the rest of the string from position pos if len is not within the length of the rest of the string. Returns NULL if any argument is NULL.

## Syntax

```sql
INSERT(str,pos,len,newstr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str | The string. |
| pos | The position. |
| len | The length. |
| newstr | The new string. |

## Return Type

A string data type value.

## Examples

```txt
SELECT INSERT('Quadratic', 3, 4, 'What');
+-----------------------------------+
| INSERT('Quadratic', 3, 4, 'What') |
+-----------------------------------+
| QuWhattic                         |
+-----------------------------------+

SELECT INSERT('Quadratic', -1, 4, 'What');
+---------------------------------------+
| INSERT('Quadratic', (- 1), 4, 'What') |
+---------------------------------------+
| Quadratic                             |
+---------------------------------------+

SELECT INSERT('Quadratic', 3, 100, 'What');
+-------------------------------------+
| INSERT('Quadratic', 3, 100, 'What') |
+-------------------------------------+
| QuWhat                              |
+-------------------------------------+

+--------------------------------------------+--------+
| INSERT('123456789', number, number, 'aaa') | number |
+--------------------------------------------+--------+
| 123456789                                  |      0 |
| aaa23456789                                |      1 |
| 1aaa456789                                 |      2 |
| 12aaa6789                                  |      3 |
| 123aaa89                                   |      4 |
| 1234aaa                                    |      5 |
| 12345aaa                                   |      6 |
| 123456aaa                                  |      7 |
| 1234567aaa                                 |      8 |
| 12345678aaa                                |      9 |
| 123456789                                  |     10 |
| 123456789                                  |     11 |
| 123456789                                  |     12 |
+--------------------------------------------+--------+
```
