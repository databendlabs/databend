---
title: UNHEX
---

For a string argument str, UNHEX(str) interprets each pair of characters in the argument as a hexadecimal number and converts it to the byte represented by the number. The return value is a binary string.

## Syntax

```sql
UNHEX(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr        | The string. |

## Examples

```sql
SELECT UNHEX('6461746162656e64');
+---------------------------+
| UNHEX('6461746162656e64') |
+---------------------------+
| databend                  |
+---------------------------+

SELECT UNHEX(HEX('string'));
+----------------------+
| UNHEX(HEX('string')) |
+----------------------+
| string               |
+----------------------+
```
