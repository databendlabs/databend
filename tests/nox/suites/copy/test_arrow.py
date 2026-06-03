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
