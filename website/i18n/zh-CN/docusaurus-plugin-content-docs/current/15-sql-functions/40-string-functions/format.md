---
title: FORMAT
---

Formats the number X to a format like '#,###,###.##', rounded to D decimal places, and returns the result as a string.

The optional third parameter enables a locale to be specified to be used for the result number's decimal point, thousands separator, and grouping between separators.

## Syntax

```sql
FORMAT(X, D, [locale])
```

## Return Type

A string.

## Examples

```sql
SELECT FORMAT(12332.123456, 4);
+-------------------------+
| FORMAT(12332.123456, 4) |
+-------------------------+
| 12,332.1235             |
+-------------------------+

SELECT FORMAT(12332.1,4);
+--------------------+
| FORMAT(12332.1, 4) |
+--------------------+
| 12,332.1000        |
+--------------------+

SELECT FORMAT(12332.2,0);
+--------------------+
| FORMAT(12332.2, 0) |
+--------------------+
| 12,332             |
+--------------------+

SELECT FORMAT(12332.2,2,'de_DE');
+-----------------------------+
| FORMAT(12332.2, 2, 'de_DE') |
+-----------------------------+
| 12,332.20                   |
+-----------------------------+
```
