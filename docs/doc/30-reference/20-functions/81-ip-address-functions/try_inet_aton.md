---
title: TRY_INET_ATON
---

try_inet_aton function is used to take the dotted-quad representation of an IPv4 address as a string and returns the numeric value of the given IP address in form of an integer.

## Syntax

```sql
TRY_INET_ATON( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | String.     |

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
