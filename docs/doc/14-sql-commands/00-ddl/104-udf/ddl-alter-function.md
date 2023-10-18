---
title: ALTER FUNCTION
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.116"/>

Alters a user-defined function.

## Syntax

```sql
ALTER FUNCTION [IF NOT EXISTS] <function_name> 
    AS (<input_param_names>) -> <lambda_expression> 
    [DESC='<description>']
```

## Examples

```sql
-- Create a UDF
CREATE FUNCTION a_plus_3 AS (a) -> a+3+3;

-- Modify the lambda expression of the UDF
ALTER FUNCTION a_plus_3 AS (a) -> a+3;
```