import pytest


def _put_file_to_stage(conn, stage_name, path):
    conn.exec(f"put fs://{path} @{stage_name}")


@pytest.mark.parametrize(
    "format_name, delimiter",
    [("csv", ","), ("text", "\t")],
)
def test_stage_read_with_non_utf8_encoding(copy_env, tmp_path, format_name, delimiter):
    conn = copy_env.conn
    stage_name = copy_env.uniq_name
    table_name = f"{stage_name}_t"
    path = tmp_path / f"{format_name}_gbk.data"
    path.write_bytes(f"张三{delimiter}1\n".encode("gbk"))

    conn.exec(
        f"create or replace stage {stage_name} "
        f"file_format=(type={format_name} encoding='gbk')"
    )
    _put_file_to_stage(conn, stage_name, path)

    conn.exec(f"create or replace table {table_name} (name string, id int)")
    res = conn.query_row(f"copy into {table_name} from @{stage_name}")
    assert res.values()[1] == 1
    assert conn.query_row(f"select * from {table_name}").values() == ("张三", 1)

    assert conn.query_row(f"select $1, $2 from @{stage_name}").values() == ("张三", "1")


@pytest.mark.parametrize(
    "format_name, delimiter",
    [("csv", ","), ("text", "\t")],
)
def test_stage_read_utf8_encoding_error_modes(copy_env, tmp_path, format_name, delimiter):
    conn = copy_env.conn
    name = copy_env.uniq_name
    strict_stage = f"{name}_strict"
    replace_stage = f"{name}_replace"
    strict_table = f"{strict_stage}_t"
    replace_table = f"{replace_stage}_t"
    path = tmp_path / f"{format_name}_mixed_utf8.data"
    path.write_bytes(b"ab\xffcd" + delimiter.encode() + b"1\n")

    conn.exec(
        f"create or replace stage {strict_stage} "
        f"file_format=(type={format_name} encoding='utf8' encoding_error='strict')"
    )
    _put_file_to_stage(conn, strict_stage, path)
    conn.exec(f"create or replace table {strict_table} (name string, id int)")

    with pytest.raises(Exception):
        conn.query_row(f"select $1 from @{strict_stage}")
    with pytest.raises(Exception):
        conn.query_row(f"copy into {strict_table} from @{strict_stage}")

    conn.exec(
        f"create or replace stage {replace_stage} "
        f"file_format=(type={format_name} encoding='utf8' encoding_error='replace')"
    )
    _put_file_to_stage(conn, replace_stage, path)
    conn.exec(f"create or replace table {replace_table} (name string, id int)")

    assert conn.query_row(f"select $1, $2 from @{replace_stage}").values() == (
        "ab\ufffdcd",
        "1",
    )

    res = conn.query_row(f"copy into {replace_table} from @{replace_stage}")
    assert res.values()[1] == 1
    assert conn.query_row(f"select * from {replace_table}").values() == ("ab\ufffdcd", 1)
