---
id: string-substring
title: SUBSTRING
---

SUBSTRING function is used to extract a string containing a specific number of characters from a particular position of a given string.

## Syntax

```sql
SUBSTRING(<expr> [FROM <position_expr>] [FOR <length_expr>])
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | The main string from where the character to be extracted |
| position_expr | The one-indexed position expression to start at. If negative, counts from the end |
| length_expr | The number expression of characters to extract |

## Return Type

String

!!! note
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


## Examples

```
mysql> SELECT SUBSTRING('1234567890' FROM 3 FOR 3);
+---------------------------+
| SUBSTRING(1234567890,3,3) |
+---------------------------+
| 345                       |
+---------------------------+

mysql> SELECT SUBSTRING('1234567890' FROM 3);
+------------------------------+
| SUBSTRING(1234567890,3,NULL) |
+------------------------------+
| 34567890                     |
+------------------------------+
1 row in set (0.01 sec)

mysql> SELECT SUBSTRING('1234567890' FOR 3);
+---------------------------+
| SUBSTRING(1234567890,1,3) |
+---------------------------+
| 123                       |
+---------------------------+
1 row in set (0.01 sec)

```
