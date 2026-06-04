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
            f"select id, to_string(items) from {table_name} order by id"
        )
    ]
    assert rows == [
        (1, "[(10,NULL,1),(20,NULL,2)]"),
        (2, None),
        (3, "[]"),
        (4, "[(40,NULL,4)]"),
    ]

    selected = [
        row.values()
        for row in conn.query_iter(
            f"select id, to_string(items) from @{stage_name} order by id"
        )
    ]
    assert selected == [
        (1, "[(1,10),(2,20)]"),
        (2, None),
        (3, "[]"),
        (4, "[(4,40)]"),
    ]

    res = conn.query_row(
        f"copy into {select_table_name} "
        f"from (select id + 10, items from @{stage_name} where id in (1, 4))"
    )
    assert res.values()[1] == 2

    copied_from_select = [
        row.values()
        for row in conn.query_iter(
            f"select id, to_string(items) from {select_table_name} order by id"
        )
    ]
    assert copied_from_select == [
        (11, "[(1,10),(2,20)]"),
        (14, "[(4,40)]"),
    ]
