import os
import platform
import uuid
from datetime import date, datetime
from decimal import Decimal
from urllib.parse import urljoin

import pyarrow as pa
import pytest
import requests


QUERY_URL = "http://localhost:8000/v1/query"
QUERY_AUTH = ("root", "root")


def _quote_sql_string(value):
    return str(value).replace("'", "''")


def _stage_fs_url(path):
    return f"fs://{_quote_sql_string(path)}/"


def _execute_sql(sql):
    response = requests.post(
        QUERY_URL,
        auth=QUERY_AUTH,
        json={"sql": sql, "pagination": {"wait_time_secs": 10}},
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()

    while True:
        if result.get("error"):
            raise RuntimeError(f"query failed for {sql}: {result['error']}")

        next_uri = result.get("next_uri")
        if not next_uri:
            return

        response = requests.get(
            urljoin(QUERY_URL, next_uri),
            auth=QUERY_AUTH,
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()


@pytest.mark.skipif(
    platform.system() == "Darwin"
    and platform.machine() == "arm64"
    and int(pa.__version__.split(".", 1)[0]) <= 8,
    reason="legacy PyArrow Parquet wheels are not usable on macOS arm64",
)
@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_parquet_unload_is_readable_by_configured_pyarrow(tmp_path, compression):
    import pyarrow.parquet as pq

    expected_version = os.environ["PYARROW_COMPAT_VERSION"]
    assert pa.__version__ == expected_version

    stage_name = f"pyarrow_compat_{uuid.uuid4().hex[:8]}"
    output_path = tmp_path / "unload.parquet"

    _execute_sql(
        f"create stage {stage_name} "
        f"url='{_stage_fs_url(tmp_path)}' "
        f"file_format=(type=parquet compression={compression})"
    )
    try:
        _execute_sql(
            f"copy into @{stage_name}/unload.parquet from ("
            "select "
            "true as b, "
            "cast(7 as int) as i32, "
            "cast(42 as bigint) as i64, "
            "cast(9 as uint64) as u64, "
            "cast(1.25 as double) as f64, "
            "'legacy'::string as s, "
            "to_binary('bytes') as bin, "
            "cast(123.45 as decimal(18, 2)) as d64, "
            "cast('12345678901234567890.12' as decimal(30, 2)) as d128, "
            "cast('12345678901234567890123456789012345678.90' "
            "as decimal(40, 2)) as d256, "
            "to_date('2021-01-02') as d, "
            "to_timestamp('2021-01-02 03:04:05') as ts, "
            "cast(null as string) as nullable_s, "
            "['nested'] as nested, "
            "map(['key'], ['value']) as map_value, "
            "[0.25, 0.5]::vector(2) as vector_value"
            f") file_format=(type=parquet compression={compression}) "
            "single=true use_raw_path=true overwrite=true"
        )

        table = pq.read_table(output_path)

        assert table.schema.field("b").type == pa.bool_()
        assert table.schema.field("i32").type == pa.int32()
        assert table.schema.field("i64").type == pa.int64()
        assert table.schema.field("u64").type == pa.uint64()
        assert table.schema.field("f64").type == pa.float64()
        assert table.schema.field("s").type == pa.string()
        assert table.schema.field("bin").type == pa.large_binary()
        assert table.schema.field("d64").type == pa.decimal128(18, 2)
        assert table.schema.field("d128").type == pa.decimal128(30, 2)
        assert table.schema.field("d256").type == pa.decimal256(40, 2)
        assert table.schema.field("d").type == pa.date32()
        assert table.schema.field("ts").type == pa.timestamp("us")
        assert table.schema.field("nullable_s").type == pa.string()
        nested_type = table.schema.field("nested").type
        assert pa.types.is_large_list(nested_type)
        assert nested_type.value_type == pa.string()
        map_type = table.schema.field("map_value").type
        assert pa.types.is_map(map_type)
        assert map_type.key_type == pa.string()
        assert map_type.item_type == pa.string()
        vector_type = table.schema.field("vector_value").type
        assert pa.types.is_fixed_size_list(vector_type)
        assert vector_type.list_size == 2
        assert vector_type.value_type == pa.float32()

        assert table.num_rows == 1
        row = {name: values[0] for name, values in table.to_pydict().items()}
        assert row["b"] is True
        assert row["i32"] == 7
        assert row["i64"] == 42
        assert row["u64"] == 9
        assert row["f64"] == 1.25
        assert row["s"] == "legacy"
        assert row["bin"] == b"bytes"
        assert row["d64"] == Decimal("123.45")
        assert row["d128"] == Decimal("12345678901234567890.12")
        assert row["d256"] == Decimal("12345678901234567890123456789012345678.90")
        assert row["d"] == date(2021, 1, 2)
        assert row["ts"] == datetime(2021, 1, 2, 3, 4, 5)
        assert row["nullable_s"] is None
        assert row["nested"] == ["nested"]
        assert row["map_value"] == [("key", "value")]
        assert row["vector_value"] == pytest.approx([0.25, 0.5])
    finally:
        _execute_sql(f"drop stage if exists {stage_name}")
