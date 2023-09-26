# Copyright 2023 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from concurrent.futures import ThreadPoolExecutor
from typing import *
import concurrent
import inspect
import json
import pyarrow as pa
import pyarrow.flight
import traceback

# comes from Databend
MAX_DECIMAL128_PRECISION = 38
MAX_DECIMAL256_PRECISION = 76
EXTENSION_KEY = "Extension"
ARROW_EXT_TYPE_VARIANT = "Variant"

TIMESTAMP_UINT = "us"


class UserDefinedFunction:
    """
    Base interface for user-defined function.
    """

    _name: str
    _input_schema: pa.Schema
    _result_schema: pa.Schema

    def eval_batch(self, batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        """
        Apply the function on a batch of inputs.
        """
        return iter([])


class ScalarFunction(UserDefinedFunction):
    """
    Base interface for user-defined scalar function. A user-defined scalar functions maps zero, one,
    or multiple scalar values to a new scalar value.
    """

    _func: Callable
    _io_threads: Optional[int]
    _executor: Optional[ThreadPoolExecutor]
    _skip_null: bool

    def __init__(
        self, func, input_types, result_type, name=None, io_threads=None, skip_null=None
    ):
        self._func = func
        self._input_schema = pa.schema(
            field.with_name(arg_name)
            for arg_name, field in zip(
                inspect.getfullargspec(func)[0],
                [_to_arrow_field(t) for t in _to_list(input_types)],
            )
        )
        self._result_schema = pa.schema(
            [_to_arrow_field(result_type).with_name("output")]
        )
        self._name = name or (
            func.__name__ if hasattr(func, "__name__") else func.__class__.__name__
        )
        self._io_threads = io_threads
        self._executor = (
            ThreadPoolExecutor(max_workers=self._io_threads)
            if self._io_threads is not None
            else None
        )

        if skip_null and not self._result_schema.field(0).nullable:
            raise ValueError(
                f"Return type of function {self._name} must be nullable when skip_null is True"
            )

        self._skip_null = skip_null or False
        super().__init__()

    def eval_batch(self, batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        inputs = [[v.as_py() for v in array] for array in batch]
        inputs = [
            _process_func(pa.list_(type), False)(array)
            for array, type in zip(inputs, self._input_schema.types)
        ]
        if self._executor is not None:
            # concurrently evaluate the function for each row
            if self._skip_null:
                null_func = lambda *v: None
                tasks = []
                for row in range(batch.num_rows):
                    args = [col[row] for col in inputs]
                    func = null_func if None in args else self._func
                    tasks.append(self._executor.submit(func, *args))
            else:
                tasks = [
                    self._executor.submit(self._func, *[col[row] for col in inputs])
                    for row in range(batch.num_rows)
                ]
            column = [future.result() for future in tasks]
        else:
            # evaluate the function for each row
            if self._skip_null:
                column = []
                for row in range(batch.num_rows):
                    args = [col[row] for col in inputs]
                    column.append(None if None in args else self._func(*args))
            else:
                column = [
                    self._func(*[col[row] for col in inputs])
                    for row in range(batch.num_rows)
                ]

        column = _process_func(pa.list_(self._result_schema.types[0]), True)(column)

        array = pa.array(column, type=self._result_schema.types[0])
        yield pa.RecordBatch.from_arrays([array], schema=self._result_schema)

    def __call__(self, *args):
        return self._func(*args)


def udf(
    input_types: Union[List[Union[str, pa.DataType]], Union[str, pa.DataType]],
    result_type: Union[str, pa.DataType],
    name: Optional[str] = None,
    io_threads: Optional[int] = None,
    skip_null: Optional[bool] = False,
) -> Callable:
    """
    Annotation for creating a user-defined scalar function.

    Parameters:
    - input_types: A list of strings or Arrow data types that specifies the input data types.
    - result_type: A string or an Arrow data type that specifies the return value type.
    - name: An optional string specifying the function name. If not provided, the original name will be used.
    - io_threads: Number of I/O threads used per data chunk for I/O bound functions.
    - skip_null: A boolean value specifying whether to skip NULL value. If it is set to True, NULL values
                will not be passed to the function, and the corresponding return value is set to NULL. Default to False.

    Example:
    ```
    @udf(input_types=['INT', 'INT'], result_type='INT')
    def gcd(x, y):
        while y != 0:
            (x, y) = (y, x % y)
        return x
    ```

    I/O bound Example:
    ```
    @udf(input_types=['INT'], result_type='INT', io_threads=64)
    def external_api(x):
        response = requests.get(my_endpoint + '?param=' + x)
        return response["data"]
    ```
    """

    if io_threads is not None and io_threads > 1:
        return lambda f: ScalarFunction(
            f,
            input_types,
            result_type,
            name,
            io_threads=io_threads,
            skip_null=skip_null,
        )
    else:
        return lambda f: ScalarFunction(
            f, input_types, result_type, name, skip_null=skip_null
        )


class UDFServer(pa.flight.FlightServerBase):
    """
    A server that provides user-defined functions to clients.

    Example:
    ```
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(my_udf)
    server.serve()
    ```
    """

    _location: str
    _functions: Dict[str, UserDefinedFunction]

    def __init__(self, location="0.0.0.0:8815", **kwargs):
        super(UDFServer, self).__init__("grpc://" + location, **kwargs)
        self._location = location
        self._functions = {}

    def get_flight_info(self, context, descriptor):
        """Return the result schema of a function."""
        func_name = descriptor.path[0].decode("utf-8")
        if func_name not in self._functions:
            raise ValueError(f"Function {func_name} does not exists")
        udf = self._functions[func_name]
        # return the concatenation of input and output schema
        full_schema = pa.schema(list(udf._input_schema) + list(udf._result_schema))
        return pa.flight.FlightInfo(
            schema=full_schema,
            descriptor=descriptor,
            endpoints=[],
            total_records=len(full_schema),
            total_bytes=0,
        )

    def do_exchange(self, context, descriptor, reader, writer):
        """Call a function from the client."""
        func_name = descriptor.path[0].decode("utf-8")
        if func_name not in self._functions:
            raise ValueError(f"Function {func_name} does not exists")
        udf = self._functions[func_name]
        writer.begin(udf._result_schema)
        try:
            for batch in reader:
                for output_batch in udf.eval_batch(batch.data):
                    writer.write_batch(output_batch)
        except Exception as e:
            print(traceback.print_exc())
            raise e

    def add_function(self, udf: UserDefinedFunction):
        """Add a function to the server."""
        name = udf._name
        if name in self._functions:
            raise ValueError("Function already exists: " + name)
        self._functions[name] = udf
        input_types = ", ".join(
            _arrow_field_to_string(field) for field in udf._input_schema
        )
        output_type = _arrow_field_to_string(udf._result_schema[0])
        sql = f"CREATE FUNCTION {name} ({input_types}) RETURNS {output_type} LANGUAGE python HANDLER = '{name}' ADDRESS = 'http://{self._location}';"
        print(f"added function: {name}, corresponding SQL:\n{sql}\n")

    def serve(self):
        """Start the server."""
        print(f"listening on {self._location}")
        super(UDFServer, self).serve()


def _process_func(type: pa.DataType, output: bool) -> Callable:
    """
    Return a function to process input or output value.

    For input type:
        - String=pa.string(): bytes -> str
        - Tuple=pa.struct(): dict -> tuple
        - Json=pa.large_binary(): bytes -> Any
        - Map=pa.map_(): list[tuple(k,v)] -> dict

    For output type:
        - Json=pa.large_binary(): Any -> str
        - Map=pa.map_(): dict -> list[tuple(k,v)]
    """
    if pa.types.is_list(type):
        func = _process_func(type.value_type, output)
        return (
            lambda array: [(func(v) if v is not None else None) for v in array]
            if array is not None
            else None
        )
    if pa.types.is_struct(type):
        funcs = [_process_func(field.type, output) for field in type]
        if output:
            return (
                lambda tup: tuple(
                    (func(v) if v is not None else None) for v, func in zip(tup, funcs)
                )
                if tup is not None
                else None
            )
        else:
            # the input value of struct type is a dict
            # we convert it into tuple here
            return (
                lambda map: tuple(
                    (func(v) if v is not None else None)
                    for v, func in zip(map.values(), funcs)
                )
                if map is not None
                else None
            )
    if pa.types.is_map(type):
        funcs = [
            _process_func(type.key_type, output),
            _process_func(type.item_type, output),
        ]
        if output:
            # dict -> list[tuple[k,v]]
            return (
                lambda map: [
                    tuple(func(v) for v, func in zip(item, funcs))
                    for item in map.items()
                ]
                if map is not None
                else None
            )
        else:
            # list[tuple[k,v]] -> dict
            return (
                lambda array: dict(
                    tuple(func(v) for v, func in zip(item, funcs)) for item in array
                )
                if array is not None
                else None
            )

    if pa.types.is_string(type) and not output:
        # string type is converted to LargeBinary in Databend,
        # we cast it back to string here
        return lambda v: v.decode("utf-8") if v is not None else None
    if pa.types.is_large_binary(type):
        if output:
            return lambda v: json.dumps(v) if v is not None else None
        else:
            return lambda v: json.loads(v) if v is not None else None
    return lambda v: v


def _to_list(x):
    if isinstance(x, list):
        return x
    else:
        return [x]


def _to_arrow_field(t: Union[str, pa.DataType]) -> pa.Field:
    """
    Convert a string or pyarrow.DataType to pyarrow.Field.
    """
    if isinstance(t, str):
        return _type_str_to_arrow_field(t)
    else:
        return pa.field("", t, False)


def _type_str_to_arrow_field(type_str: str) -> pa.Field:
    """
    Convert a SQL data type to `pyarrow.Field`.
    """
    type_str = type_str.strip().upper()
    nullable = True
    if type_str.endswith("NULL"):
        type_str = type_str[:-4].strip()
        if type_str.endswith("NOT"):
            type_str = type_str[:-3].strip()
            nullable = False

    return _type_str_to_arrow_field_inner(type_str).with_nullable(nullable)


def _type_str_to_arrow_field_inner(type_str: str) -> pa.Field:
    type_str = type_str.strip().upper()
    if type_str in ("BOOLEAN", "BOOL"):
        return pa.field("", pa.bool_(), False)
    elif type_str in ("TINYINT", "INT8"):
        return pa.field("", pa.int8(), False)
    elif type_str in ("SMALLINT", "INT16"):
        return pa.field("", pa.int16(), False)
    elif type_str in ("INT", "INTEGER", "INT32"):
        return pa.field("", pa.int32(), False)
    elif type_str in ("BIGINT", "INT64"):
        return pa.field("", pa.int64(), False)
    elif type_str in ("TINYINT UNSIGNED", "UINT8"):
        return pa.field("", pa.uint8(), False)
    elif type_str in ("SMALLINT UNSIGNED", "UINT16"):
        return pa.field("", pa.uint16(), False)
    elif type_str in ("INT UNSIGNED", "INTEGER UNSIGNED", "UINT32"):
        return pa.field("", pa.uint32(), False)
    elif type_str in ("BIGINT UNSIGNED", "UINT64"):
        return pa.field("", pa.uint64(), False)
    elif type_str in ("FLOAT", "FLOAT32"):
        return pa.field("", pa.float32(), False)
    elif type_str in ("FLOAT64", "DOUBLE"):
        return pa.field("", pa.float64(), False)
    elif type_str == "DATE":
        return pa.field("", pa.date32(), False)
    elif type_str in ("DATETIME", "TIMESTAMP"):
        return pa.field("", pa.timestamp(TIMESTAMP_UINT), False)
    elif type_str in ("STRING", "VARCHAR", "CHAR", "CHARACTER", "TEXT"):
        return pa.field("", pa.string(), False)
    elif type_str in ("VARIANT", "JSON"):
        # In Databend, JSON type is identified by the "EXTENSION" key in the metadata.
        return pa.field(
            "",
            pa.large_binary(),
            nullable=False,
            metadata={EXTENSION_KEY: ARROW_EXT_TYPE_VARIANT},
        )
    elif type_str.startswith("NULLABLE"):
        type_str = type_str[8:].strip("()").strip()
        return _type_str_to_arrow_field_inner(type_str).with_nullable(True)
    elif type_str.endswith("NULL"):
        type_str = type_str[:-4].strip()
        return _type_str_to_arrow_field_inner(type_str).with_nullable(True)
    elif type_str.startswith("DECIMAL"):
        # DECIMAL(precision, scale)
        str_list = type_str[7:].strip("()").split(",")
        precision = int(str_list[0].strip())
        scale = int(str_list[1].strip())
        if precision < 1 or precision > MAX_DECIMAL256_PRECISION:
            raise ValueError(
                f"Decimal precision must be between 1 and {MAX_DECIMAL256_PRECISION}"
            )
        elif scale > precision:
            raise ValueError(
                f"Decimal scale must be between 0 and precision {precision}"
            )

        if precision < MAX_DECIMAL128_PRECISION:
            return pa.field("", pa.decimal128(precision, scale), False)
        else:
            return pa.field("", pa.decimal256(precision, scale), False)
    elif type_str.startswith("ARRAY"):
        # ARRAY(INT)
        type_str = type_str[5:].strip("()").strip()
        return pa.field("", pa.list_(_type_str_to_arrow_field_inner(type_str)), False)
    elif type_str.startswith("MAP"):
        # MAP(STRING, INT)
        str_list = type_str[3:].strip("()").split(",")
        key_field = _type_str_to_arrow_field_inner(str_list[0].strip())
        val_field = _type_str_to_arrow_field_inner(str_list[1].strip())
        return pa.field("", pa.map_(key_field, val_field), False)
    elif type_str.startswith("TUPLE"):
        # TUPLE(STRING, INT, INT)
        str_list = type_str[5:].strip("()").split(",")
        fields = []
        for type_str in str_list:
            type_str = type_str.strip()
            fields.append(_type_str_to_arrow_field_inner(type_str))
        return pa.field("", pa.struct(fields), False)
    else:
        raise ValueError(f"Unsupported type: {type_str}")


def _arrow_field_to_string(field: pa.Field) -> str:
    """
    Convert a `pyarrow.Field` to a SQL data type string.
    """
    type_str = _data_type_to_string(field.type)
    return f"{type_str} NOT NULL" if not field.nullable else type_str


def _inner_field_to_string(field: pa.Field) -> str:
    ## inner field default is NOT NULL in databend
    type_str = _data_type_to_string(field.type)
    return f"{type_str} NULL" if field.nullable else type_str


def _data_type_to_string(t: pa.DataType) -> str:
    """
    Convert a `pyarrow.DataType` to a SQL data type string.
    """
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    elif pa.types.is_int8(t):
        return "TINYINT"
    elif pa.types.is_int16(t):
        return "SMALLINT"
    elif pa.types.is_int32(t):
        return "INT"
    elif pa.types.is_int64(t):
        return "BIGINT"
    elif pa.types.is_uint8(t):
        return "TINYINT UNSIGNED"
    elif pa.types.is_uint16(t):
        return "SMALLINT UNSIGNED"
    elif pa.types.is_uint32(t):
        return "INT UNSIGNED"
    elif pa.types.is_uint64(t):
        return "BIGINT UNSIGNED"
    elif pa.types.is_float32(t):
        return "FLOAT"
    elif pa.types.is_float64(t):
        return "DOUBLE"
    elif pa.types.is_decimal(t):
        return f"DECIMAL({t.precision}, {t.scale})"
    elif pa.types.is_date32(t):
        return "DATE"
    elif pa.types.is_timestamp(t):
        return "TIMESTAMP"
    elif pa.types.is_string(t):
        return "VARCHAR"
    elif pa.types.is_large_binary(t):
        return "VARIANT"
    elif pa.types.is_list(t):
        return f"ARRAY({_inner_field_to_string(t.value_field)})"
    elif pa.types.is_map(t):
        return f"MAP({_inner_field_to_string(t.key_field)}, {_inner_field_to_string(t.item_field)})"
    elif pa.types.is_struct(t):
        args_str = ", ".join(_inner_field_to_string(field) for field in t)
        return f"TUPLE({args_str})"
    else:
        raise ValueError(f"Unsupported type: {t}")
