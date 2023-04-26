---
title: CHAR_LENGTH
---

Returns the length of the string str, measured in characters.
A multibyte character counts as a single character.
This means that for a string containing five 2-byte characters, LENGTH() returns 10, whereas CHAR_LENGTH() returns 5.

## Syntax

```sql
CHAR_LENGTH(<expr>);
```

## Arguments

| Arguments | Description |
|-----------| ----------- |
| `<expr>`  | The string  |

## Return Type

`BIGINT`

## Examples

```sql
SELECT CHAR_LENGTH('hello');
+----------------------+
| CHAR_LENGTH('hello') |
+----------------------+
|                    5 |
+----------------------+
```
