---
id: string-bit_length
title: BIT_LENGTH
---

Return the length of a string in bits.

## Syntax

```sql
BIT_LENGTH(<expr>)
```

## Arguments

| Arguments | Description |
|-----------| ----------- |
| `<expr>`  | The string. |

## Return Type

`BIGINT`

## Examples

```sql
SELECT BIT_LENGTH('Word');
+----------------------------+
| SELECT BIT_LENGTH('Word'); |
+----------------------------+
| 32                         |
+----------------------------+
```
