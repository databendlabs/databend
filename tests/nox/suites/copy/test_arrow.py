from decimal import Decimal

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest


def _quote_sql_string(value):
    value = str(value)
    return value.replace("'", "''")


def _stage_fs_url(path):
    return f"fs://{_quote_sql_string(path)}/"


def _write_arrow(path, stream):
    table = pa.table({"id": [1, 2], "name": ["alice", "bob"]})
    with path.open("wb") as f:
        writer = (
            ipc.new_stream(f, table.schema)
            if stream
            else ipc.new_file(f, table.schema)
        )
        with writer:
            writer.write_table(table)


def _write_arrow_string_ids(path, stream):
    table = pa.table({"id": ["bad"], "name": ["broken"]})
    with path.open("wb") as f:
        writer = (
            ipc.new_stream(f, table.schema)
            if stream
            else ipc.new_file(f, table.schema)
        )
        with writer:
            writer.write_table(table)


def _write_arrow_batches(path, stream, batches):
    with path.open("wb") as f:
        writer = (
            ipc.new_stream(f, batches[0].schema)
            if stream
            else ipc.new_file(f, batches[0].schema)
        )
        with writer:
            for batch in batches:
                writer.write_batch(batch)


def _timestamp_tz_value(timestamp_micros, offset_seconds=0):
    return Decimal((offset_seconds << 64) | timestamp_micros)


def _extension_batches():
    timestamp_tz_type = pa.decimal128(38, 0)
    timestamp_tz_field = pa.field(
        "ts",
        timestamp_tz_type,
        metadata={b"Extension": b"TimestampTz"},
    )
    timestamp_tz_string_field = pa.field(
        "ts_string",
        pa.string(),
    )
    events_type = pa.list_(
        pa.struct(
            [
                timestamp_tz_field,
                timestamp_tz_string_field,
                pa.field("label", pa.string()),
            ]
        )
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            timestamp_tz_field,
            timestamp_tz_string_field,
            pa.field("events", events_type),
        ]
    )
    first_ts = _timestamp_tz_value(1_735_689_600_000_000)
    second_ts = _timestamp_tz_value(1_735_693_200_000_000)
    third_ts = _timestamp_tz_value(1_735_776_000_000_000)

    return [
        pa.record_batch(
            [
                pa.array([1, 2], type=pa.int32()),
                pa.array([first_ts, second_ts], type=timestamp_tz_type),
                pa.array(
                    [
                        "2025-01-01 00:00:00 +0000",
                        "2025-01-01 01:00:00 +0000",
                    ],
                    type=pa.string(),
                ),
                pa.array(
                    [
                        [
                            {
                                "ts": first_ts,
                                "ts_string": "2025-01-01 00:00:00 +0000",
                                "label": "start",
                            }
                        ],
                        [
                            {
                                "ts": second_ts,
                                "ts_string": "2025-01-01 01:00:00 +0000",
                                "label": "middle",
                            },
                            {
                                "ts": third_ts,
                                "ts_string": "2025-01-02 00:00:00 +0000",
                                "label": "end",
                            },
                        ],
                    ],
                    type=events_type,
                ),
            ],
            schema=schema,
        )
    ]


def _nested_batches():
    items_type = pa.list_(
        pa.struct(
            [
                pa.field("x", pa.int32()),
                pa.field("y", pa.int32()),
            ]
        )
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("items", items_type),
        ]
    )

    return [
        pa.record_batch(
            [
                pa.array([1, 2], type=pa.int32()),
                pa.array(
                    [
                        [{"x": 1, "y": 10}, {"x": 2, "y": 20}],
                        None,
                    ],
                    type=items_type,
                ),
            ],
            schema=schema,
        ),
        pa.record_batch(
            [
                pa.array([3, 4], type=pa.int32()),
                pa.array(
                    [
                        [],
                        [{"x": 4, "y": 40}],
                    ],
                    type=items_type,
                ),
            ],
            schema=schema,
        ),
    ]


@pytest.mark.parametrize(
    "format_name, stream",
    [("arrow", False), ("arrow_stream", True)],
)
def test_copy_into_table_from_arrow(copy_env, tmp_path, format_name, stream):
    conn = copy_env.conn
    stage_name = f"{copy_env.uniq_name}_{format_name}"
    table_name = f"{stage_name}_t"
    stage_dir = tmp_path / stage_name
    stage_dir.mkdir()
    _write_arrow(stage_dir / f"data.{format_name}", stream)

    conn.exec(
        f"create or replace stage {stage_name} "
        f"url='{_stage_fs_url(stage_dir)}' "
        f"file_format=(type={format_name} missing_field_as=field_default)"
    )
    conn.exec(
        f"create or replace table {table_name} "
        "(id int, name string, extra int default 7)"
    )

    res = conn.query_row(f"copy into {table_name} from @{stage_name}")
    assert res.values()[1] == 2
    rows = [row.values() for row in conn.query_iter(f"select * from {table_name} order by id")]
    assert rows == [(1, "alice", 7), (2, "bob", 7)]

    selected = [
        row.values()
        for row in conn.query_iter(f"select id, name from @{stage_name} order by id")
    ]
    assert selected == [(1, "alice"), (2, "bob")]


@pytest.mark.parametrize(
    "format_name",
    ["arrow", "arrow_stream"],
)
def test_copy_into_stage_as_arrow(copy_env, tmp_path, format_name):
    conn = copy_env.conn
    stage_name = f"{copy_env.uniq_name}_{format_name}"
    table_name = f"{stage_name}_unload_t"
    stage_dir = tmp_path / stage_name
    stage_dir.mkdir()

    conn.exec(
        f"create or replace stage {stage_name} "
        f"url='{_stage_fs_url(stage_dir)}' "
        f"file_format=(type={format_name} missing_field_as=field_default)"
    )
    conn.exec(
        f"copy into @{stage_name}/unload "
        "from (select 1 as id, 'alice' as name union all select 2, 'bob') "
        f"file_format=(type={format_name}) single=true use_raw_path=true overwrite=true"
    )
    conn.exec(
        f"create or replace table {table_name} "
        "(id int, name string, extra int default 7)"
    )

    selected = [
        row.values()
        for row in conn.query_iter(
            f"select id, name from @{stage_name}/unload order by id"
        )
    ]
    assert selected == [(1, "alice"), (2, "bob")]

    res = conn.query_row(f"copy into {table_name} from @{stage_name}/unload")
    assert res.values()[1] == 2
    rows = [row.values() for row in conn.query_iter(f"select * from {table_name} order by id")]
    assert rows == [(1, "alice", 7), (2, "bob", 7)]


@pytest.mark.parametrize(
    "format_name",
    ["arrow", "arrow_stream"],
)
def test_empty_arrow_stage_count(copy_env, tmp_path, format_name):
    conn = copy_env.conn
    stage_name = f"{copy_env.uniq_name}_{format_name}"
    stage_dir = tmp_path / stage_name
    stage_dir.mkdir()
    (stage_dir / "empty.arrow").touch()

    conn.exec(
        f"create or replace stage {stage_name} "
        f"url='{_stage_fs_url(stage_dir)}' "
        f"file_format=(type={format_name} missing_field_as=field_default)"
    )

    assert conn.query_row(f"select count(*) from @{stage_name}/missing").values()[0] == 0
    assert conn.query_row(f"select count(*) from @{stage_name}").values()[0] == 0
    with pytest.raises(Exception, match="no non-empty Arrow file to infer schema"):
        conn.query_row(f"select id from @{stage_name}/missing")
    with pytest.raises(Exception, match="no non-empty Arrow file to infer schema"):
        conn.query_row(f"select id from @{stage_name}")


@pytest.mark.parametrize(
    "format_name, stream",
    [("arrow", False), ("arrow_stream", True)],
)
def test_copy_arrow_on_error_continue(copy_env, tmp_path, format_name, stream):
    conn = copy_env.conn
    stage_name = f"{copy_env.uniq_name}_{format_name}"
    table_name = f"{stage_name}_on_error_t"
    stage_dir = tmp_path / stage_name
    stage_dir.mkdir()
    _write_arrow(stage_dir / f"valid.{format_name}", stream)
    _write_arrow_string_ids(stage_dir / f"bad_cast.{format_name}", stream)
    (stage_dir / f"corrupt.{format_name}").write_bytes(b"not an arrow ipc file")

    conn.exec(
        f"create or replace stage {stage_name} "
        f"url='{_stage_fs_url(stage_dir)}' "
        f"file_format=(type={format_name} missing_field_as=field_default)"
    )
    conn.exec(f"create or replace table {table_name} (id int, name string)")

    result = [
        row.values()
        for row in conn.query_iter(
            f"copy into {table_name} from @{stage_name} on_error=continue"
        )
    ]
    failed = [row for row in result if row[2] == 1]
    assert len(failed) == 2
    assert any(row[0].endswith(f"bad_cast.{format_name}") for row in failed)
    assert any(row[0].endswith(f"corrupt.{format_name}") for row in failed)

    rows = [row.values() for row in conn.query_iter(f"select * from {table_name} order by id")]
    assert rows == [(1, "alice"), (2, "bob")]


@pytest.mark.parametrize(
    "format_name, stream",
    [("arrow", False), ("arrow_stream", True)],
)
def test_copy_nested_arrow_batches(copy_env, tmp_path, format_name, stream):
    conn = copy_env.conn
    stage_name = f"{copy_env.uniq_name}_{format_name}"
    table_name = f"{stage_name}_nested_t"
    select_table_name = f"{stage_name}_nested_select_t"
    stage_dir = tmp_path / stage_name
    stage_dir.mkdir()
    _write_arrow_batches(stage_dir / f"nested.{format_name}", stream, _nested_batches())

    conn.exec(
        f"create or replace stage {stage_name} "
        f"url='{_stage_fs_url(stage_dir)}' "
        f"file_format=(type={format_name} missing_field_as=field_default)"
    )
    conn.exec(
        f"create or replace table {table_name} "
        "(id int, items array(tuple(y int, z int, x int)))"
    )
    conn.exec(
        f"create or replace table {select_table_name} "
        "(id int, items array(tuple(x int, y int)))"
    )

    res = conn.query_row(f"copy into {table_name} from @{stage_name}")
    assert res.values()[1] == 4

    rows = [
        row.values()
        for row in conn.query_iter(
            f"select id, length(items), items[1].y, items[1].z, items[1].x, "
            f"items[2].y, items[2].z, items[2].x from {table_name} order by id"
        )
    ]
    assert rows == [
        (1, 2, 10, None, 1, 20, None, 2),
        (2, None, None, None, None, None, None, None),
        (3, 0, None, None, None, None, None, None),
        (4, 1, 40, None, 4, None, None, None),
    ]

    selected = [
        row.values()
        for row in conn.query_iter(
            f"select id, length(items), items[1].x, items[1].y, "
            f"items[2].x, items[2].y from @{stage_name} order by id"
        )
    ]
    assert selected == [
        (1, 2, 1, 10, 2, 20),
        (2, None, None, None, None, None),
        (3, 0, None, None, None, None),
        (4, 1, 4, 40, None, None),
    ]

    conn.exec(
        f"copy into {select_table_name} "
        f"from (select id + 10, items from @{stage_name})"
    )
    assert conn.query_row(f"select count(*) from {select_table_name}").values()[0] == 4

    copied_from_select = [
        row.values()
        for row in conn.query_iter(
            f"select id, length(items), items[1].x, items[1].y, "
            f"items[2].x, items[2].y from {select_table_name} order by id"
        )
    ]
    assert copied_from_select == [
        (11, 2, 1, 10, 2, 20),
        (12, None, None, None, None, None),
        (13, 0, None, None, None, None),
        (14, 1, 4, 40, None, None),
    ]


@pytest.mark.parametrize(
    "format_name, stream",
    [("arrow", False), ("arrow_stream", True)],
)
def test_copy_arrow_extension_field_metadata(copy_env, tmp_path, format_name, stream):
    conn = copy_env.conn
    stage_name = f"{copy_env.uniq_name}_{format_name}"
    table_name = f"{stage_name}_extension_t"
    select_table_name = f"{stage_name}_extension_select_t"
    stage_dir = tmp_path / stage_name
    stage_dir.mkdir()
    _write_arrow_batches(
        stage_dir / f"extension.{format_name}",
        stream,
        _extension_batches(),
    )

    conn.exec(
        f"create or replace stage {stage_name} "
        f"url='{_stage_fs_url(stage_dir)}' "
        f"file_format=(type={format_name} missing_field_as=field_default)"
    )
    conn.exec(
        f"create or replace table {table_name} "
        "(id int, ts timestamp_tz, ts_string timestamp_tz, "
        "events array(tuple(ts timestamp_tz, ts_string timestamp_tz, label string)))"
    )
    conn.exec(
        f"create or replace table {select_table_name} "
        "(id int, ts timestamp_tz, ts_string timestamp_tz, "
        "events array(tuple(ts timestamp_tz, ts_string timestamp_tz, label string)))"
    )

    inferred_type = conn.query_row(
        f"select typeof(ts), typeof(ts_string) from @{stage_name} limit 1"
    )
    assert inferred_type.values() == ("TIMESTAMP_TZ NULL", "VARCHAR NULL")

    stage_rows = [
        row.values()
        for row in conn.query_iter(
            f"select id, ts::string, ts_string::string, length(events), "
            f"events[1].ts::string, events[1].ts_string::string, events[1].label, "
            f"events[2].ts::string, events[2].ts_string::string, events[2].label "
            f"from @{stage_name} order by id"
        )
    ]
    assert stage_rows == [
        (
            1,
            "2025-01-01 00:00:00.000000 +0000",
            "2025-01-01 00:00:00 +0000",
            1,
            "2025-01-01 00:00:00.000000 +0000",
            "2025-01-01 00:00:00 +0000",
            "start",
            None,
            None,
            None,
        ),
        (
            2,
            "2025-01-01 01:00:00.000000 +0000",
            "2025-01-01 01:00:00 +0000",
            2,
            "2025-01-01 01:00:00.000000 +0000",
            "2025-01-01 01:00:00 +0000",
            "middle",
            "2025-01-02 00:00:00.000000 +0000",
            "2025-01-02 00:00:00 +0000",
            "end",
        ),
    ]

    res = conn.query_row(f"copy into {table_name} from @{stage_name}")
    assert res.values()[1] == 2

    copied_rows = [
        row.values()
        for row in conn.query_iter(
            f"select id, ts::string, ts_string::string, length(events), "
            f"events[1].ts::string, events[1].ts_string::string, events[1].label, "
            f"events[2].ts::string, events[2].ts_string::string, events[2].label "
            f"from {table_name} order by id"
        )
    ]
    assert copied_rows == [
        (
            1,
            "2025-01-01 00:00:00.000000 +0000",
            "2025-01-01 00:00:00.000000 +0000",
            1,
            "2025-01-01 00:00:00.000000 +0000",
            "2025-01-01 00:00:00.000000 +0000",
            "start",
            None,
            None,
            None,
        ),
        (
            2,
            "2025-01-01 01:00:00.000000 +0000",
            "2025-01-01 01:00:00.000000 +0000",
            2,
            "2025-01-01 01:00:00.000000 +0000",
            "2025-01-01 01:00:00.000000 +0000",
            "middle",
            "2025-01-02 00:00:00.000000 +0000",
            "2025-01-02 00:00:00.000000 +0000",
            "end",
        ),
    ]

    conn.exec(
        f"copy into {select_table_name} "
        f"from (select id + 10, ts, ts_string, events from @{stage_name})"
    )
    assert conn.query_row(f"select count(*) from {select_table_name}").values()[0] == 2

    copied_from_select = [
        row.values()
        for row in conn.query_iter(
            f"select id, ts::string, ts_string::string, length(events), "
            f"events[1].ts::string, events[1].ts_string::string, events[1].label, "
            f"events[2].ts::string, events[2].ts_string::string, events[2].label "
            f"from {select_table_name} order by id"
        )
    ]
    assert copied_from_select == [
        (
            11,
            "2025-01-01 00:00:00.000000 +0000",
            "2025-01-01 00:00:00.000000 +0000",
            1,
            "2025-01-01 00:00:00.000000 +0000",
            "2025-01-01 00:00:00.000000 +0000",
            "start",
            None,
            None,
            None,
        ),
        (
            12,
            "2025-01-01 01:00:00.000000 +0000",
            "2025-01-01 01:00:00.000000 +0000",
            2,
            "2025-01-01 01:00:00.000000 +0000",
            "2025-01-01 01:00:00.000000 +0000",
            "middle",
            "2025-01-02 00:00:00.000000 +0000",
            "2025-01-02 00:00:00.000000 +0000",
            "end",
        )
    ]
