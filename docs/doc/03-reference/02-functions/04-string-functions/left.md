---
title: LEFT
---

Returns the leftmost len characters from the string str, or NULL if any argument is NULL.

## Syntax

```sql
LEFT(str,len);
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
SELECT LEFT('foobarbar', 5);
+----------------------+
| LEFT('foobarbar', 5) |
+----------------------+
| fooba                |
+----------------------+
```
