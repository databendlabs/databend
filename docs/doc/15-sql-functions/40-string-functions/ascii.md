---
title: ASCII
---

Returns the numeric value of the leftmost character of the string str.

## Syntax

```sql
ASCII(<expr>)
```

## Arguments

| Arguments | Description |
|-----------|-------------|
| `<expr>`  | The string. |

## Return Type

`TINYINT`

## Examples

```sql
SELECT ASCII('2');
+------------+
| ASCII('2') |
+------------+
|         50 |
+------------+
```
