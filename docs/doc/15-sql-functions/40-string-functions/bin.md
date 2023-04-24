---
title: BIN
---

Returns a string representation of the binary value of N.

## Syntax

```sql
BIN(<expr>)
```

## Arguments

| Arguments | Description |
|-----------|-------------|
| `<expr>`  | The number. |

## Return Type

`VARCHAR`

## Examples

```sql
SELECT BIN(12);
+---------+
| BIN(12) |
+---------+
| 1100    |
+---------+
```
