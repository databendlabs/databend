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

# https://github.com/datafuselabs/databend-udf
from databend_udf import udf, UDFServer


logging.basicConfig(level=logging.INFO)


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


@udf(input_types=["INT"], result_type="INT")
def wait(x):
    time.sleep(0.1)
    return x


@udf(input_types=["INT"], result_type="INT", io_threads=32)
def wait_concurrent(x):
    time.sleep(0.1)
    return x


if __name__ == "__main__":
    udf_server = UDFServer("0.0.0.0:8815")
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
    udf_server.serve()
