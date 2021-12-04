---
title: SHA2
---

Calculates the SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512).
If the hash length is not one of the permitted values, the return value is NULL.
Otherwise, the function result is a hash value containing the desired number of bits as a string of hexadecimal digits.

## Syntax

```sql
sha2(expression, expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | The string to be hashed. |
| expression  | Number to indicates the desired bit length of the result (224, 256, 384, 512, or 0 which is equivalent to 256). |

## Return Type

A String data type.

## Examples

```text
mysql> SELECT sha2('1234567890', 0);
+------------------------------------------------------------------+
| sha2('1234567890', 0)                                            |
+------------------------------------------------------------------+
| c775e7b757ede630cd0aa1113bd102661ab38829ca52a6422ab782862f268646 |
+------------------------------------------------------------------+
```

```text
mysql> SELECT sha2('1234567890', 256);
+------------------------------------------------------------------+
| sha2('1234567890', 0)                                            |
+------------------------------------------------------------------+
| c775e7b757ede630cd0aa1113bd102661ab38829ca52a6422ab782862f268646 |
+------------------------------------------------------------------+
```

mysql> SELECT sha2('1234567890', 1);
+-----------------------+
| sha2('1234567890', 1) |
+-----------------------+
| NULL                  |
+-----------------------+
