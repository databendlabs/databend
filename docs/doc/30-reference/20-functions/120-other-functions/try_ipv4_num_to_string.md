---
title: try_ipv4_num_to_string
---

Converts a 32-bit integer to an IPv4 address.

## Syntax

```sql
try_ipv4_num_to_string( <int32> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<int32>` | An integer.

## Return Type

String, a dotted-quad IP address, eg “1.2.3.4” or "255.255.255.255". 

## Examples

```sql
MySQL [(none)]> select try_ipv4_num_to_string(16909060);
+----------------------------------+
| try_ipv4_num_to_string(16909060) |
+----------------------------------+
| 1.2.3.4                          |
+----------------------------------+


MySQL [(none)]> select try_ipv4_num_to_string(1690906000000000000000000000);
+------------------------------------------------------+
| try_ipv4_num_to_string(1690906000000000000000000000) |
+------------------------------------------------------+
| 255.255.255.255                                      |
+------------------------------------------------------+
```
