---
title: External Function
---
import IndexOverviewList from '@site/src/components/IndexOverviewList';

External functions in Databend allow you to define custom operations for processing data. These functions are implemented using an external server in programming languages such as Python. They represent an advanced form of custom operation, relying on an external server to define and execute custom data processing operations. Key features of external functions include:

- Scalability: External functions are well-suited for handling complex and resource-intensive data operations, making them suitable for demanding processing tasks.

- External Libraries: They can utilize external libraries and dependencies, enhancing their capabilities and versatility by integrating additional functionality.

- Advanced Logic: External functions can implement advanced and sophisticated data processing logic, making them ideal for complex data processing scenarios.

## Supported Programming Languages

This table provides information on the supported languages and the necessary libraries to create External Functions in Databend.

| Language | Required Library                                                                                                                                                                                                                                                            |
|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Python   | **udf.py**: The library is not yet publicly available. Prior to its release, please download the 'udf.py' file from [this link](https://github.com/datafuselabs/databend/blob/main/tests/udf-server/udf.py) and ensure it is saved in the same directory as your Python script. |

## Managing External Functions

To manage external functions in Databend, use the following commands:

<IndexOverviewList />

## Usage Examples

This section demonstrates how to create an external function in each of the [Supported Programming Languages](#supported-programming-languages).

### Creating an External Function in Python

1. Enable external server support by adding the following parameters to the [query] section in the [databend-query.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-query.toml) configuration file.

```toml title='databend-query.toml'
[query]
...
enable_udf_server = true
# List the allowed server addresses, separating multiple addresses with commas.
# For example, ['http://0.0.0.0:8815', 'http://example.com']
udf_server_allow_list = ['http://0.0.0.0:8815']
...
```

2. Define your function. This code defines and runs an external server in Python, which exposes a custom function *gcd* for calculating the greatest common divisor of two integers and allows remote execution of this function:

```python title='external_function.py'
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
    # create an external server listening at '0.0.0.0:8815'
    server = UDFServer("0.0.0.0:8815")
    # add defined functions
    server.add_function(gcd)
    # start the external server
    server.serve()
```

`@udf` is a decorator used for defining external functions in Databend, supporting the following parameters:

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

3. Run the Python file to start the external server:

```shell
python3 external_function.py
```

4. Register the function *gcd* with the [CREATE FUNCTION](ddl-create-function.md) in Databend:

```sql
CREATE FUNCTION gcd (INT, INT) RETURNS INT LANGUAGE python HANDLER = 'gcd' ADDRESS = 'http://0.0.0.0:8815'；
```