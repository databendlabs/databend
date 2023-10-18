---
title: User-Defined Function
---
import IndexOverviewList from '@site/src/components/IndexOverviewList';

User-Defined Functions (UDFs) enable you to define your custom operations to process data within Databend. User-Defined Functions are categorized into two types: [Lambda-Based UDFs](#lambda-based-udfs) and [Server-Based UDFs](#server-based-udfs). This page explains what they are, how to manage them, and provides two usage examples.

## Lambda-Based UDFs

Lambda-based UDFs are a type of user-defined function in Databend that are implemented using anonymous lambda expressions. These UDFs are simple, lightweight, and provide a quick way to define custom operations on data within Databend.

- Customized Data Transformations: Lambda-based UDFs empower you to perform data transformations that may not be achievable through built-in Databend functions alone. This customization is particularly valuable for handling unique data formats or implementing specific business logic.

- Code Reusability: Lambda-based UDFs can be easily reused across multiple queries, saving time and effort in coding and maintaining data processing logic.

## Server-Based UDFs

Server-based UDFs are a more advanced type of user-defined function in Databend. They involve the use of a UDF server implemented in programming languages such as Python to define custom operations on data.

- Scalability: Server-based UDFs can handle complex and resource-intensive operations on data, making them suitable for more extensive and demanding processing tasks.

- External Libraries: They can leverage external libraries and dependencies, allowing you to integrate additional functionality into your UDFs, making them more powerful and versatile.

- Advanced Logic: Server-based UDFs are capable of implementing advanced and sophisticated logic, making them suitable for complex data processing scenarios.

## Managing UDFs

To manage UDFs in Databend, use the following commands:

<IndexOverviewList />

## Usage Examples

This section demonstrates two UDF implementation methods within Databend: one by creating UDFs with lambda expressions and the other by utilizing UDF servers in conjunction with Python. For additional examples of defining UDFs in various programming languages, see [CREATE FUNCTION](ddl-create-function.md).

### UDF Implementation with Lambda Expression

This example implements a UDF named *a_plus_3* using a lambda expression:

```sql
CREATE FUNCTION a_plus_3 AS (a) -> a+3;

SELECT a_plus_3(2);
+---------+
| (2 + 3) |
+---------+
|       5 |
+---------+
```

### UDF Implementation via UDF Server

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
    server = UDFServer("0.0.0.0:8815")
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
