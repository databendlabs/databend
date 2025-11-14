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

import logging
import datetime
from decimal import Decimal
import time
from typing import List, Dict, Any, Tuple, Optional
from pyarrow import flight

# https://github.com/datafuselabs/databend-udf
from databend_udf import StageLocation, UDFServer, udf

logging.basicConfig(level=logging.INFO)


class HeadersMiddlewareFactory(flight.ServerMiddlewareFactory):
    def start_call(self, info, headers):
        return HeadersMiddleware(headers)


class HeadersMiddleware(flight.ServerMiddleware):
    _headers: Dict[str, str]

    def __init__(self, headers, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._headers = headers

    def get_token(self, key):
        print(self._headers)
        return self._headers.get(key)


class CheckHeadersServer(UDFServer):
    def do_exchange(self, context, descriptor, reader, writer):
        headers = context.get_middleware("headers")

        if descriptor.path == [b"check_headers"]:
            required_header = "x-authorization"
            expect_token = ["123"]
            token = headers.get_token(required_header)

            if token is None:
                raise flight.FlightUnauthenticatedError(
                    f"Missing required header: {required_header.upper()}"
                )
            if token != expect_token:
                raise flight.FlightUnauthenticatedError(
                    f"Wrong token(expect: {expect_token}): {token}"
                )

        return super().do_exchange(context, descriptor, reader, writer)


@udf(input_types=["TINYINT", "SMALLINT", "INT", "BIGINT"], result_type="BIGINT")
def add_signed(a, b, c, d):
    return a + b + c + d


@udf(input_types=["UINT8", "UINT16", "UINT32", "UINT64"], result_type="UINT64")
def add_unsigned(a, b, c, d):
    return a + b + c + d


@udf(input_types=["FLOAT", "DOUBLE"], result_type="DOUBLE")
def add_float(a, b):
    return a + b


@udf(input_types=["BOOLEAN", "BIGINT", "BIGINT"], result_type="BIGINT")
def bool_select(condition, a, b):
    return a if condition else b


@udf(
    name="gcd",
    input_types=["INT", "INT"],
    result_type="INT",
    skip_null=True,
)
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x


gcd_error_cnt = 0


@udf(
    name="gcd_error",
    input_types=["INT", "INT"],
    result_type="INT",
    skip_null=True,
)
def gcd_error(x: int, y: int) -> int:
    global gcd_error_cnt
    if y % 2 == 0 and gcd_error_cnt <= 3:
        gcd_error_cnt += 1
        raise ValueError("gcd_error")
    while y != 0:
        (x, y) = (y, x % y)
    return x


@udf(input_types=["VARCHAR", "VARCHAR", "VARCHAR"], result_type="VARCHAR")
def split_and_join(s: str, split_s: str, join_s: str) -> str:
    return join_s.join(s.split(split_s))


@udf(input_types=["BINARY"], result_type="BINARY")
def binary_reverse(s: bytes) -> bytes:
    return s[::-1]


@udf(input_types="VARCHAR", result_type="DECIMAL(36, 18)")
def hex_to_dec(hex: str) -> Decimal:
    hex = hex.strip()

    dec = Decimal(0)
    while hex:
        chunk = hex[:16]
        chunk_value = int(hex[:16], 16)
        dec = dec * (1 << (4 * len(chunk))) + chunk_value
        chunk_len = len(chunk)
        hex = hex[chunk_len:]
    return dec


@udf(input_types=["DECIMAL(36, 18)", "DECIMAL(36, 18)"], result_type="DECIMAL(72, 28)")
def decimal_div(v1: Decimal, v2: Decimal) -> Decimal:
    result = v1 / v2
    return result.quantize(Decimal("0." + "0" * 28))


@udf(input_types=["DATE", "INT"], result_type="DATE")
def add_days_py(dt: datetime.date, days: int):
    return dt + datetime.timedelta(days=days)


@udf(input_types=["TIMESTAMP", "INT"], result_type="TIMESTAMP")
def add_hours_py(dt: datetime.datetime, hours: int):
    return dt + datetime.timedelta(hours=hours)


@udf(input_types=["ARRAY(VARCHAR)", "INT"], result_type="VARCHAR")
def array_access(array: List[str], idx: int) -> Optional[str]:
    if idx == 0 or idx > len(array):
        return None
    return array[idx - 1]


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


@udf(input_types=["MAP(VARCHAR,VARCHAR)", "VARCHAR"], result_type="VARCHAR")
def map_access(map: Dict[str, str], key: str) -> str:
    return map[key] if key in map else None


@udf(input_types=["VARIANT", "VARCHAR"], result_type="VARIANT")
def json_access(data: Any, key: str) -> Any:
    return data[key]


@udf(input_types=["VARCHAR"], result_type="BIGINT")
def url_len(key: str) -> int:
    return len(key)


@udf(input_types=["VARCHAR"], result_type="BIGINT")
def url_len_mul_100(key: str) -> int:
    return len(key) * 100


@udf(input_types=["ARRAY(VARIANT)"], result_type="VARIANT")
def json_concat(list: List[Any]) -> Any:
    return list


@udf(
    input_types=["TUPLE(ARRAY(VARIANT NULL), INT, VARCHAR)", "INT", "INT"],
    result_type="TUPLE(VARIANT NULL, VARIANT NULL)",
)
def tuple_access(
    tup: Tuple[List[Any], int, str], idx1: int, idx2: int
) -> Tuple[Any, Any]:
    v1 = None if idx1 == 0 or idx1 > len(tup) else tup[idx1 - 1]
    v2 = None if idx2 == 0 or idx2 > len(tup) else tup[idx2 - 1]
    return v1, v2


ALL_SCALAR_TYPES = [
    "BOOLEAN",
    "TINYINT",
    "SMALLINT",
    "INT",
    "BIGINT",
    "UINT8",
    "UINT16",
    "UINT32",
    "UINT64",
    "FLOAT",
    "DOUBLE",
    "DATE",
    "TIMESTAMP",
    "VARCHAR",
    "VARIANT",
]


@udf(
    input_types=ALL_SCALAR_TYPES,
    result_type=f"TUPLE({','.join(f'{t} NULL' for t in ALL_SCALAR_TYPES)})",
)
def return_all(
    bool,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    date,
    timestamp,
    varchar,
    json,
):
    return (
        bool,
        i8,
        i16,
        i32,
        i64,
        u8,
        u16,
        u32,
        u64,
        f32,
        f64,
        date,
        timestamp,
        varchar,
        json,
    )


@udf(
    input_types=[f"ARRAY({t})" for t in ALL_SCALAR_TYPES],
    result_type=f"TUPLE({','.join(f'ARRAY({t})' for t in ALL_SCALAR_TYPES)})",
)
def return_all_arrays(
    bool,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    date,
    timestamp,
    varchar,
    json,
):
    return (
        bool,
        i8,
        i16,
        i32,
        i64,
        u8,
        u16,
        u32,
        u64,
        f32,
        f64,
        date,
        timestamp,
        varchar,
        json,
    )


@udf(
    input_types=[f"{t} NOT NULL" for t in ALL_SCALAR_TYPES],
    result_type=f"TUPLE({','.join(f'{t}' for t in ALL_SCALAR_TYPES)})",
)
def return_all_non_nullable(
    bool,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    date,
    timestamp,
    varchar,
    json,
):
    return (
        bool,
        i8,
        i16,
        i32,
        i64,
        u8,
        u16,
        u32,
        u64,
        f32,
        f64,
        date,
        timestamp,
        varchar,
        json,
    )


from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time

executor = ThreadPoolExecutor()
executor2 = ProcessPoolExecutor()


def cal_multi_thread(row):
    r = sum(range(3500000 + row))
    return r


def cal_sleep(row):
    time.sleep(0.2)
    return row


@udf(input_types=["INT"], result_type="BIGINT", batch_mode=True)
def fib_async(x: List[int]) -> List[int]:
    current_thread = threading.current_thread()
    print("batchs {} {}".format(len(x), current_thread.name))

    tasks = [executor2.submit(cal_multi_thread, row) for row in x]
    result = [future.result() for future in tasks]

    tasks2 = [executor.submit(lambda row: cal_sleep(row), row) for row in x]
    result2 = [future.result() for future in tasks]

    print("batchs done {}".format(len(x)))
    return result


import threading


@udf(input_types=["INT"], result_type="BIGINT", batch_mode=True)
def fib(x: List[int]) -> List[int]:
    current_thread = threading.current_thread()
    print("batchs {} {}".format(len(x), current_thread.name))
    result = [sum(range(3500000 + row)) for row in x]
    print("batchs done {}".format(len(x)))
    # tasks = [executor.submit(lambda row: cal_sleep(row), row) for row in x]
    # _column = [future.result() for future in tasks]
    return result


@udf(input_types=["INT"], result_type="INT")
def wait(x):
    time.sleep(0.1)
    return x


@udf(input_types=["INT"], result_type="INT", io_threads=32)
def wait_concurrent(x):
    time.sleep(0.1)
    return x


@udf(input_types=["VARCHAR"], result_type="VARCHAR")
def ping(s: str) -> str:
    return s


@udf(input_types=[], result_type="VARCHAR")
def check_headers() -> str:
    return "success"


@udf(input_types=["VARCHAR"], result_type="ARRAY(FLOAT32 NULL)")
def embedding_4(s: str):
    return [1.1, 1.2, 1.3, 1.4]


def _stage_bucket(stage: StageLocation) -> str:
    storage = stage.storage or {}
    return storage.get("bucket") or storage.get("container") or ""


@udf(stage_refs=["data_stage"], input_types=["INT"], result_type="VARCHAR")
def stage_summary(data_stage: StageLocation, value: int) -> str:
    assert data_stage.stage_type.lower() == "external"
    assert data_stage.storage
    bucket = _stage_bucket(data_stage)
    return f"{data_stage.stage_name}:{bucket}:{data_stage.relative_path}:{value}"


@udf(
    stage_refs=["data_stage"],
    input_types=["INT"],
    result_type=[
        ("stage_name", "VARCHAR"),
        ("stage_type", "VARCHAR"),
        ("bucket", "VARCHAR"),
        ("relative_path", "VARCHAR"),
        ("value", "INT"),
        ("summary", "VARCHAR"),
    ],
)
def stage_summary_udtf(data_stage: StageLocation, value: int):
    assert data_stage.stage_type.lower() == "external"
    assert data_stage.storage
    bucket = _stage_bucket(data_stage)
    summary = f"{data_stage.stage_name}:{bucket}:{data_stage.relative_path}:{value}"
    return [
        {
            "stage_name": data_stage.stage_name or "",
            "stage_type": data_stage.stage_type or "",
            "bucket": bucket,
            "relative_path": data_stage.relative_path or "",
            "value": value,
            "summary": summary,
        }
    ]


@udf(
    stage_refs=["input_stage", "output_stage"],
    input_types=["INT"],
    result_type="INT",
)
def multi_stage_process(
    input_stage: StageLocation, output_stage: StageLocation, value: int
) -> int:
    assert input_stage.storage and output_stage.storage
    assert input_stage.stage_type.lower() == "external"
    assert output_stage.stage_type.lower() == "external"
    # Simple deterministic behaviour for testing
    return (
        value
        + len(input_stage.storage.get("bucket", ""))
        + len(output_stage.storage.get("bucket", ""))
    )


@udf(
    stage_refs=["input_stage", "output_stage"],
    input_types=["INT"],
    result_type=[
        ("input_stage", "VARCHAR"),
        ("output_stage", "VARCHAR"),
        ("input_bucket", "VARCHAR"),
        ("output_bucket", "VARCHAR"),
        ("input_relative_path", "VARCHAR"),
        ("output_relative_path", "VARCHAR"),
        ("result", "INT"),
    ],
)
def multi_stage_process_udtf(
    input_stage: StageLocation, output_stage: StageLocation, value: int
):
    assert input_stage.storage and output_stage.storage
    assert input_stage.stage_type.lower() == "external"
    assert output_stage.stage_type.lower() == "external"
    input_bucket = _stage_bucket(input_stage)
    output_bucket = _stage_bucket(output_stage)
    result = value + len(input_bucket) + len(output_bucket)
    return [
        {
            "input_stage": input_stage.stage_name or "",
            "output_stage": output_stage.stage_name or "",
            "input_bucket": input_bucket,
            "output_bucket": output_bucket,
            "input_relative_path": input_stage.relative_path or "",
            "output_relative_path": output_stage.relative_path or "",
            "result": result,
        }
    ]


@udf(
    stage_refs=["input_stage", "output_stage"],
    input_types=["VARCHAR"],
    result_type="INT",
)
def immutable_multi_stage_process(
    input_stage: StageLocation, output_stage: StageLocation, value: str
) -> int:
    assert input_stage.storage and output_stage.storage
    assert input_stage.stage_type.lower() == "external"
    assert output_stage.stage_type.lower() == "external"
    # Simple deterministic behaviour for testing
    return (
        len(value)
        + len(input_stage.storage.get("bucket", ""))
        + len(output_stage.storage.get("bucket", ""))
    )


if __name__ == "__main__":
    udf_server = CheckHeadersServer(
        location="0.0.0.0:8815", middleware={"headers": HeadersMiddlewareFactory()}
    )
    udf_server.add_function(add_signed)
    udf_server.add_function(add_unsigned)
    udf_server.add_function(add_float)
    udf_server.add_function(binary_reverse)
    udf_server.add_function(bool_select)
    udf_server.add_function(gcd)
    udf_server.add_function(gcd_error)
    udf_server.add_function(split_and_join)
    udf_server.add_function(decimal_div)
    udf_server.add_function(hex_to_dec)
    udf_server.add_function(add_days_py)
    udf_server.add_function(add_hours_py)
    udf_server.add_function(array_access)
    udf_server.add_function(array_index_of)
    udf_server.add_function(map_access)
    udf_server.add_function(json_access)
    udf_server.add_function(json_concat)
    udf_server.add_function(tuple_access)
    udf_server.add_function(return_all)
    udf_server.add_function(return_all_arrays)
    udf_server.add_function(return_all_non_nullable)
    udf_server.add_function(wait)
    udf_server.add_function(wait_concurrent)
    udf_server.add_function(url_len)
    udf_server.add_function(url_len_mul_100)
    udf_server.add_function(check_headers)
    udf_server.add_function(embedding_4)
    udf_server.add_function(stage_summary)
    udf_server.add_function(stage_summary_udtf)
    udf_server.add_function(multi_stage_process)
    udf_server.add_function(multi_stage_process_udtf)
    udf_server.add_function(immutable_multi_stage_process)

    # Built-in function
    udf_server.add_function(ping)
    udf_server.serve()
