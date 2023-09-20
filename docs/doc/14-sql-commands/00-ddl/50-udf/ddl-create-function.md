---
title: CREATE FUNCTION
description:
  Create a new user-defined scalar function.
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.116"/>

Creates a user-defined function.

## Syntax

```sql
-- Create with lambda expression
CREATE FUNCTION [IF NOT EXISTS] <function_name> AS (<input_param_names>) -> <lambda_expression> [DESC='<description>']

-- Create with UDF server
CREATE FUNCTION [IF NOT EXISTS] <function_name> AS (<input_param_types>) RETURNS <return_type> LANGUAGE <language_name> HANDLER = '<handler_name>' ADDRESS = '<udf_server_address>' [DESC='<description>']
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

### Creating with Lambda Expression

```sql
CREATE FUNCTION a_plus_3 AS (a) -> a+3;

SELECT a_plus_3(2);
+---------+
| (2 + 3) |
+---------+
|       5 |
+---------+
```

```sql
-- Define lambda-style UDF
CREATE FUNCTION get_v1 AS (json) -> json["v1"];
CREATE FUNCTION get_v2 AS (json) -> json["v2"];

-- Create a time series table
CREATE TABLE json_table(time TIMESTAMP, data JSON);

-- Insert a time event
INSERT INTO json_table VALUES('2022-06-01 00:00:00.00000', PARSE_JSON('{"v1":1.5, "v2":20.5}'));

-- Get v1 and v2 value from the event
SELECT get_v1(data), get_v2(data) FROM json_table;
+------------+------------+
| data['v1'] | data['v2'] |
+------------+------------+
| 1.5        | 20.5       |
+------------+------------+

DROP FUNCTION get_v1;

DROP FUNCTION get_v2;

DROP TABLE json_table;
```

### Creating with UDF Server (Python)

This example demonstrates how to enable and configure a UDF server in Python:

1. Enable UDF server support by adding the following parameters to the [query] section in the [databend-query.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-query.toml) configuration file.

```toml title='databend-query.toml'
[query]
...
enable_udf_server = true
# List the allowed UDF server addresses, separating multiple addresses with commas.
# For example, ['http://0.0.0.0:8815', 'http://example.com']
udf_server_allow_list = ['http://0.0.0.0:8815']
...
```

2. Define your function. This code defines and runs a UDF server in Python, which exposes a custom function *gcd* for calculating the greatest common divisor of two integers and allows remote execution of this function:

:::note
The SDK package is not yet available. Prior to its release, please download the 'udf.py' file from https://github.com/datafuselabs/databend/blob/main/tests/udf-server/udf.py and ensure it is saved in the same directory as this Python script. This step is essential for the code to function correctly.
:::

```python title='udf_server.py'
from udf import *

@udf(
    input_types=["INT", "INT"],
    result_type="INT",
    skip_null=True,
)
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x

if __name__ == '__main__':
    # create a UDF server listening at '0.0.0.0:8815'
    server = UdfServer("0.0.0.0:8815")
    # add defined functions
    server.add_function(gcd)
    # start the UDF server
    server.serve()
```

`@udf` is a decorator used for defining UDFs in Databend, supporting the following parameters:

| Parameter    | Description                                                                                         |
|--------------|-----------------------------------------------------------------------------------------------------|
| input_types  | A list of strings or Arrow data types that specify the input data types.                          |
| result_type  | A string or an Arrow data type that specifies the return value type.                                |
| name         | An optional string specifying the function name. If not provided, the original name will be used. |
| io_threads   | Number of I/O threads used per data chunk for I/O bound functions.                                    |
| skip_null    | A boolean value specifying whether to skip NULL values. If set to True, NULL values will not be passed to the function, and the corresponding return value is set to NULL. Default is False. |

This table illustrates the correspondence between Databend data types and their corresponding Python equivalents:

| Databend Type         | Python Type          |
|-----------------------|-----------------------|
| BOOLEAN               | bool                  |
| TINYINT (UNSIGNED)    | int                   |
| SMALLINT (UNSIGNED)   | int                   |
| INT (UNSIGNED)        | int                   |
| BIGINT (UNSIGNED)     | int                   |
| FLOAT                 | float                 |
| DOUBLE                | float                 |
| DECIMAL               | decimal.Decimal       |
| DATE                  | datetime.date         |
| TIMESTAMP             | datetime.datetime     |
| VARCHAR               | str                   |
| VARIANT               | any                   |
| MAP(K,V)              | dict                  |
| ARRAY(T)              | list[T]               |
| TUPLE(T...)           | tuple(T...)           |

3. Run the Python file to start the UDF server:

```shell
python3 udf_server.py
```

4. Register the function *gcd* with the [CREATE FUNCTION](ddl-create-function.md) in Databend:

```sql
CREATE FUNCTION gcd (INT, INT) RETURNS INT LANGUAGE python HANDLER = 'gcd' ADDRESS = 'http://0.0.0.0:8815'；
```