---
title: TRY_IPV4_NUM_TO_STRING
---

Converts a 32-bit integer to an IPv4 address.

## Syntax

```sql
TRY_IPV4_NUM_TO_STRING( <int32> )
```

## Arguments

| Arguments       | Description |
| --------------- | ----------- |
| `<int32>` | An integer. |

## Return Type

String, a dotted-quad IP address, eg “1.2.3.4” or "255.255.255.255".

## Examples

```sql
SELECT try_ipv4_num_to_string(16909060);
+----------------------------------+
| try_ipv4_num_to_string(16909060) |
+----------------------------------+
| 1.2.3.4                          |
+----------------------------------+


SELECT try_ipv4_num_to_string(1690906000000000000000000000);
+------------------------------------------------------+
| try_ipv4_num_to_string(1690906000000000000000000000) |
+------------------------------------------------------+
| 255.255.255.255                                      |
+------------------------------------------------------+
```
