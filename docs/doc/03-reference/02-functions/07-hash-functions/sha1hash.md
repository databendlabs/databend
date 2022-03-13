---
title: SHA1
---

Calculates an SHA-1 160-bit checksum for the string, as described in RFC 3174 (Secure Hash Algorithm).
The value is returned as a string of 40 hexadecimal digits or NULL if the argument was NULL.

## Syntax

```sql
sha(expression)
sha1(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | The string value. |

## Return Type

A String data type.

## Examples

```text
mysql> SELECT sha('1234567890');
+------------------------------------------+
| sha1('1234567890')                       |
+------------------------------------------+
| 01b307acba4f54f55aafc33bb06bbbf6ca803e9a |
+------------------------------------------+
```

```text
mysql> SELECT sha1('1234567890');
+------------------------------------------+
| sha1('1234567890')                       |
+------------------------------------------+
| 01b307acba4f54f55aafc33bb06bbbf6ca803e9a |
+------------------------------------------+
```
