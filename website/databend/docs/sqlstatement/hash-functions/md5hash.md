---
title: MD5
---

Calculates an MD5 128-bit checksum for the string.
The value is returned as a string of 32 hexadecimal digits or NULL if the argument was NULL.

## Syntax

```sql
md5(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | The string value. |

## Return Type

A String data type hash value.

## Examples

```text
mysql> SELECT MD5('1234567890');
+----------------------------------+
| MD5('1234567890')                |
+----------------------------------+
| e807f1fcf82d132f9bb018ca6738a19f |
+----------------------------------+
```
