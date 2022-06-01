---
title: ALTER FUNCTION
description:
  Modifies the properties for an existing user-defined function.
---

## Syntax

```sql
CREATE FUNCTION <name> AS ([ argname ]) -> '<function_definition>'
```

## Examples

```sql
ALTER FUNCTION a_plus_3 AS (a) -> a+6;

SELECT a_plus_3(2);
+---------+
| (2 + 6) |
+---------+
|       8 |
+---------+
```
