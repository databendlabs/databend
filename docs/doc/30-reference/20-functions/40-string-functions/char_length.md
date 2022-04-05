---
title: CHAR_LENGTH
---

Returns the length of the string str, measured in characters.
A multibyte character counts as a single character.
This means that for a string containing five 2-byte characters, LENGTH() returns 10, whereas CHAR_LENGTH() returns 5.

## Syntax

```sql
CHAR_LENGTH(str);
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str | The string  |

## Return Type

Numeric data type value.

## Examples

```txt
SELECT CHAR_LENGTH('hello');
+----------------------+
| CHAR_LENGTH('hello') |
+----------------------+
|                    5 |
+----------------------+
```
