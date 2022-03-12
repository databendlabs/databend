---
title: RIGHT
---

Returns the rightmost len characters from the string str, or NULL if any argument is NULL.

## Syntax

```sql
RIGHT(str,len);
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str | The main string from where the character to be extracted |
| len | The count of characters |

## Return Type

String data type value.

## Examples

```txt
SELECT RIGHT('foobarbar', 4);
+-----------------------+
| RIGHT('foobarbar', 4) |
+-----------------------+
| rbar                  |
+-----------------------+
```
