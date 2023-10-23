---
title: INET_NTOA
---

Converts a 32-bit integer to an IPv4 address.

## Syntax

```sql
INET_NOTA( <int32> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<int32>` | An integer.

## Return Type

String, a dotted-quad IP address, eg “1.2.3.4” or "255.255.255.255"  if the IP address cannot be parsed.

## Examples

```sql
SELECT inet_ntoa(16909060);
+---------------------+
| inet_ntoa(16909060) |
+---------------------+
| 1.2.3.4             |
+---------------------+


SELECT Inet_ntoa(1690906000000000000000000000);
+-----------------------------------------+
| inet_ntoa(1690906000000000000000000000) |
+-----------------------------------------+
| 255.255.255.255                         |
+-----------------------------------------+
```
