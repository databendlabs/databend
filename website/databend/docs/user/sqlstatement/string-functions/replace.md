---
title: REPLACE
---

Returns the string str with all occurrences of the string from_str replaced by the string to_str.

## Syntax

```sql
REPLACE(str,from_str,to_str)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str | The string. |
| from_str | The from string. |
| to_str | The to string. |

## Return Type

A String data type value.

## Examples

```txt
SELECT REPLACE('www.mysql.com', 'w', 'Ww');
+-------------------------------------+
| REPLACE('www.mysql.com', 'w', 'Ww') |
+-------------------------------------+
| WwWwWw.mysql.com                    |
+-------------------------------------+
```
