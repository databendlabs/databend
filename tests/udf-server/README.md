## Databend UDF Server Tests

```sh
pip install pyarrow
# start UDF server
python3 udf_test.py
```

```sh
./target/debug/databend-sqllogictests --run_dir udf_server
```

## Databend Python UDF Server API
This library provides a Python API for creating user-defined functions (UDF) server in Databend.

### Introduction
Databend supports user-defined functions implemented as external functions. With the Databend Python UDF API, users can define custom UDFs using Python and start a Python process as a UDF server. Then users can call the customized UDFs in Databend. Databend will remotely access the UDF server to execute the defined functions.

### Usage

#### 1. Define your functions in a Python file
```python
from udf import *

# Define a function
@udf(input_types=["VARCHAR", "VARCHAR", "VARCHAR"], result_type="VARCHAR")
def split_and_join(s: str, split_s: str, join_s: str) -> str:
    return join_s.join(s.split(split_s))

# Define a function that accpets nullable values, and set skip_null to True to enable it returns NULL if any argument is NULL.
@udf(
    input_types=["INT", "INT"],
    result_type="INT",
    skip_null=True,
)
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x

# Define a function that accpets nullable values, and set skip_null to False to enable it handles NULL values inside the function.
@udf(
    input_types=["ARRAY(INT64 NULL)", "INT64"],
    result_type="INT NOT NULL",
    skip_null=False,
)
def array_index_of(array: List[int], item: int):
    if array is None:
        return 0

    try:
        return array.index(item) + 1
    except ValueError:
        return 0

# Define a function which is IO bound, and set io_threads to enable it can be executed concurrently.
@udf(input_types=["INT"], result_type="INT", io_threads=32)
def wait_concurrent(x):
    # assume an IO operation cost 2s
    time.sleep(2)
    return x

if __name__ == '__main__':
    # create a UDF server listening at '0.0.0.0:8815'
    server = UdfServer("0.0.0.0:8815")
    # add defined functions
    server.add_function(split_and_join)
    server.add_function(gcd)
    server.add_function(array_index_of)
    server.add_function(wait_concurrent)
    # start the UDF server
    server.serve()
```

`@udf` is an annotation for creating a UDF. It supports following parameters:

- input_types: A list of strings or Arrow data types that specifies the input data types.
- result_type: A string or an Arrow data type that specifies the return value type.
- name: An optional string specifying the function name. If not provided, the original name will be used.
- io_threads: Number of I/O threads used per data chunk for I/O bound functions.
- skip_null: A boolean value specifying whether to skip NULL value. If it is set to True, NULL values will not be passed to the function, and the corresponding return value is set to NULL. Default to False.

#### 2. Start the UDF Server
Then we can Start the UDF Server by running:
```sh
python3 udf_server.py
```

#### 3. Update Databend query node config
Now, udf server is disabled by default in databend. You can enable it by setting 'enable_udf_server = true' in query node config.

In addition, for security reasons, only the address specified in the config can be accessed by databend. The list of allowed udf server addresses are specified through the `udf_server_allowlist` variable in the query node config.

Here is an example config:
```
[query]
...
enable_udf_server = true
udf_server_allow_list = [ "http://0.0.0.0:8815", "http://example.com" ]
```

#### 4. Add the functions to Databend
We can use the `CREATE FUNCTION` command to add the functions you defined to Databend:
```
CREATE FUNCTION [IF NOT EXISTS] <udf_name> (<arg_type>, ...) RETURNS <return_type> LANGUAGE <language> HANDLER=<handler> ADDRESS=<udf_server_address>
```
The `udf_name` is the name of UDF you declared in Databend. The `handler` is the function name you defined in the python UDF server.

For example:
```sql
CREATE FUNCTION split_and_join (VARCHAR, VARCHAR, VARCHAR) RETURNS VARCHAR LANGUAGE python HANDLER = 'split_and_join' ADDRESS = 'http://0.0.0.0:8815';
```

NOTE: The udf_server_address you specify must appear in `udf_server_allow_list` explained in the previous step.

> In step 2, when you starting the UDF server, the corresponding sql statement of each function will be printed out. You can use them directly.

#### 5. Use the functions in Databend
```
mysql> select split_and_join('3,5,7', ',', ':');
+-----------------------------------+
| split_and_join('3,5,7', ',', ':') |
+-----------------------------------+
| 3:5:7                             |
+-----------------------------------+
```

### Data Types
The data types supported by the Python UDF API and their corresponding python types are as follows :

| SQL Type            | Python Type       |
| ------------------- | ----------------- |
| BOOLEAN             | bool              |
| TINYINT (UNSIGNED)  | int               |
| SMALLINT (UNSIGNED) | int               |
| INT (UNSIGNED)      | int               |
| BIGINT (UNSIGNED)   | int               |
| FLOAT               | float             |
| DOUBLE              | float             |
| DECIMAL             | decimal.Decimal   |
| DATE                | datetime.date     |
| TIMESTAMP           | datetime.datetime |
| VARCHAR             | str               |
| VARIANT             | any               |
| MAP(K,V)            | dict              |
| ARRAY(T)            | list[T]           |
| TUPLE(T...)         | tuple(T...)       |

The NULL in sql is represented by None in Python.

### Acknowledgement
Databend Python UDF Server API is inspired by [RisingWave Python API](https://pypi.org/project/risingwave/).