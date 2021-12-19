---
id: string-soundex
title: SOUNDEX
---

Returns a soundex string from str. Two strings that sound almost the same should have identical soundex strings. A standard soundex string is four characters long, but the SOUNDEX() function returns an arbitrarily long string. You can use SUBSTRING() on the result to get a standard soundex string. All nonalphabetic characters in str are ignored. All international alphabetic characters outside the A-Z range are treated as vowels.
## Syntax

```sql
SOUNDEX(str)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str | The string. |

## Return Type

A string data type value or null.

## Examples

```txt
SELECT SOUNDEX('hello');
+-----------------------------------+
| `SOUNDEX('hello')`                |
+-----------------------------------+
| H400                              |
+-----------------------------------+

SELECT SOUNDEX('international');;
+---------------------------------------+
| `SOUNDEX('international')`            |
+---------------------------------------+
| I5365354                              |
+---------------------------------------+

SELECT SOUNDEX('你quadratically');
+-------------------------------------+
| `SOUNDEX('你quadratically')`        |
+-------------------------------------+
| 你236324                            |
+-------------------------------------+

SELECT SOUNDEX(NULL);
+-------------------------------------+
| `SOUNDEX(NULL)`                     |
+-------------------------------------+
| <null>                              |
+-------------------------------------+
```