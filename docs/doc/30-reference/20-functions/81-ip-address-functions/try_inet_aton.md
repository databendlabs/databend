---
title: try_inet_aton
---

try_inet_aton function is used to take the dotted-quad representation of an IPv4 address as a string and returns the numeric value of the given IP address in form of an integer.

## Syntax

```sql
try_inet_aton(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | String.     |

## Return Type

An integer.

## Examples

```sql
SELECT TRY_INET_ATON('10.0.5.9');
+---------------------------+
| TRY_INET_ATON('10.0.5.9') |
+---------------------------+
|                 167773449 |
+---------------------------+
```
