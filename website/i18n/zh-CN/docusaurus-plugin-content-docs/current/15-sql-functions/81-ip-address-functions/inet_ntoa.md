---
title: INET_ATON
---

Converts an IPv4 address to a 32-bit integer.

## Syntax

```sql
INET_ATON ( <ip> )
```

## Arguments

| Arguments    | Description                                   |
| ------------ | --------------------------------------------- |
| `<ip>` | a dotted-quad IP address string, eg “1.2.3.4” |

## Return Type

Integer

## Examples

```sql
SELECT INET_ATON('1.2.3.4');
+----------------------+
| INET_ATON('1.2.3.4') |
+----------------------+
|             16909060 |
+----------------------+


SELECT INET_ATON('127.0.0.1');
+------------------------+
| INET_ATON('127.0.0.1') |
+------------------------+
|             2130706433 |
+------------------------+
```
