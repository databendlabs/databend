---
title: ALTER FUNCTION
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.116"/>

Alters an external function.

## Syntax

```sql
ALTER FUNCTION [IF NOT EXISTS] <function_name> 
    AS (<input_param_types>) RETURNS <return_type> LANGUAGE <language_name> 
    HANDLER = '<handler_name>' ADDRESS = '<udf_server_address>' 
    [DESC='<description>']
```

| Parameter             | Description                                                                                       |
|-----------------------|---------------------------------------------------------------------------------------------------|
| `<function_name>`     | The name of the function.                                                                        |
| `<lambda_expression>` | The lambda expression or code snippet defining the function's behavior.                          |
| `DESC='<description>'`  | Description of the UDF.|
| `<<input_param_names>`| A list of input parameter names. Separated by comma.|
| `<<input_param_types>`| A list of input parameter types. Separated by comma.|
| `<return_type>`       | The return type of the function.                                                                  |
| `LANGUAGE`            | Specifies the language used to write the function. Available values: `python`.                    |
| `HANDLER = '<handler_name>'` | Specifies the name of the function's handler.                                               |
| `ADDRESS = '<udf_server_address>'` | Specifies the address of the UDF server.                                             |

## Examples

```sql
-- Create an external function
CREATE FUNCTION gcd (INT, INT) RETURNS INT LANGUAGE python HANDLER = 'gcd' ADDRESS = 'http://0.0.0.0:8815';

-- Modify the handler of the external function
ALTER FUNCTION gcd (INT, INT) RETURNS INT LANGUAGE python HANDLER = 'gcd_new' ADDRESS = 'http://0.0.0.0:8815';
```