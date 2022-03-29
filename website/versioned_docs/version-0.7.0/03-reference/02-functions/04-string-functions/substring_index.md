---
title: SUBSTRING_INDEX
---

Returns the substring from string str before count occurrences of the delimiter delim.
If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
If count is negative, everything to the right of the final delimiter (counting from the right) is returned.

## Syntax

```sql
SUBSTRING_INDEX(str,delim,count);
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str | The main string from where the character to be extracted |
| delim | The delimiter |
| count | The number of occurrences |

## Return Type

String data type value.

## Examples

```txt
SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);
+------------------------------------------+
| SUBSTRING_INDEX('www.mysql.com', '.', 2) |
+------------------------------------------+
| www.mysql                                |
+------------------------------------------+

SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2);
+----------------------------------------------+
| SUBSTRING_INDEX('www.mysql.com', '.', (- 2)) |
+----------------------------------------------+
| mysql.com                                    |
+----------------------------------------------+
```
