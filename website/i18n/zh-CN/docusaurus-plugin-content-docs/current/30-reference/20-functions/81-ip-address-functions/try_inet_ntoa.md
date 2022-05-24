---
title: TRY_INET_NTOA
---

try_inet_ntoa function is used to take the IPv4 address in network byte order and then it returns the address as a dotted-quad string representation.

## Syntax

```sql
TRY_INET_NTOA( <expr> )
```

## Arguments

| Arguments      | Description |
| -------------- | ----------- |
| `<expr>` | An Integer. |

## Return Type

String

## Examples

```sql
SELECT try_inet_ntoa(167773449);
+--------------------------+
| try_inet_ntoa(167773449) |
+--------------------------+
| 10.0.5.9                 |
+--------------------------+
```
