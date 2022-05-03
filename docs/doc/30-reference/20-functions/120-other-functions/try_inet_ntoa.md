---
title: try_inet_ntoa
---

try_inet_ntoa function is used to take the IPv4 address in network byte order and then it returns the address as a dotted-quad string representation.

## Syntax

```sql
try_inet_ntoa(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | An Integer. |

## Return Type

String

## Examples

```sql
mysql> SELECT try_inet_ntoa(167773449);
+--------------------------+
| try_inet_ntoa(167773449) |
+--------------------------+
| 10.0.5.9                 |
+--------------------------+
```
