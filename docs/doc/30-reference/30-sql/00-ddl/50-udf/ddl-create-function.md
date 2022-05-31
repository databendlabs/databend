---
title: CREATE FUNCTION
description:
  Create a new user-defined scalar function.
---


## CREATE FUNCTION

Creates a new UDF (user-defined function), the UDF can contain a SQL expression.

## Syntax

```sql
CREATE FUNCTION [ IF NOT EXISTS ] <name> AS ([ argname ]) -> '<function_definition>'
```

## Examples

```sql
CREATE FUNCTION a_plus_3 AS (a) -> a+3;

SELECT a_plus_3(2);
+---------+
| (2 + 3) |
+---------+
|       5 |
+---------+
```
