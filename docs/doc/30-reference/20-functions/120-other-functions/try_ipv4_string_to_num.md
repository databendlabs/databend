---
title: try_ipv4_string_to_num
---

Converts an IPv4 address to a 32-bit integer.

## Syntax

```sql
try_ipv4_string_to_num ( <ip> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<ip>` | a dotted-quad IP address string, eg “1.2.3.4”

## Return Type

Integer

## Examples

```sql
MySQL [(none)]> select try_ipv4_string_to_num('1.2.3.4');
+-----------------------------------+
| try_ipv4_string_to_num('1.2.3.4') |
+-----------------------------------+
|                          16909060 |
+-----------------------------------+

MySQL [(none)]> select try_ipv4_string_to_num('127.0.0.1');
+-------------------------------------+
| try_ipv4_string_to_num('127.0.0.1') |
+-------------------------------------+
|                          2130706433 |
+-------------------------------------+
```
