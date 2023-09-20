---
title: ALTER FUNCTION
description:
  Modifies the properties for an existing user-defined function.
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.116"/>

Alters a user-defined function.

## Syntax

```sql
-- Alter UDF created with lambda expression
ALTER FUNCTION [IF NOT EXISTS] <function_name> 
    AS (<input_param_names>) -> <lambda_expression> 
    [DESC='<description>']

-- Alter UDF created with UDF server
ALTER FUNCTION [IF NOT EXISTS] <function_name> 
    AS (<input_param_types>) RETURNS <return_type> LANGUAGE <language_name> 
    HANDLER = '<handler_name>' ADDRESS = '<udf_server_address>' 
    [DESC='<description>']
```

## Examples

```sql
CREATE FUNCTION a_plus_3 AS (a) -> a+3+3;
ALTER FUNCTION a_plus_3 AS (a) -> a+3;

CREATE FUNCTION gcd (INT, INT) RETURNS INT LANGUAGE python HANDLER = 'gcd' ADDRESS = 'http://0.0.0.0:8815';
ALTER FUNCTION gcd (INT, INT) RETURNS INT LANGUAGE python HANDLER = 'gcd_new' ADDRESS = 'http://0.0.0.0:8815';
```