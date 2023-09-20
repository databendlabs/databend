---
title: ALTER FUNCTION
description:
  Modifies the properties for an existing user-defined function.
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.116"/>

## Syntax

```sql
CREATE FUNCTION <name> AS ([ argname ]) -> '<function_definition>'
```

## Examples

```sql
ALTER FUNCTION a_plus_3 AS (a) -> a+3;

SELECT a_plus_3(2);
+---------+
| (2 + 3) |
+---------+
|       5 |
+---------+
```
