---
title: CREATE FUNCTION
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.116"/>

Creates an external function.

## Syntax

```sql
CREATE FUNCTION [IF NOT EXISTS] <function_name> 
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

See [Usage Examples](index.md#usage-examples).