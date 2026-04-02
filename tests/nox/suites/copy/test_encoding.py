import pytest


def _quote_sql_string(value):
    value = str(value)
    return value.replace("'", "''")


def _stage_fs_url(path):
    return f"fs://{_quote_sql_string(path)}/"


def _expected_unload_bytes(format_name, delimiter):
    if format_name == "csv":
        return f'"张三"{delimiter}1\n'.encode("gbk")
    return f"张三{delimiter}1\n".encode("gbk")


@pytest.mark.parametrize(
    "format_name, delimiter",
    [("csv", ","), ("text", "\t")],
)
def test_stage_read_with_non_utf8_encoding(copy_env, tmp_path, format_name, delimiter):
    conn = copy_env.conn
    stage_name = copy_env.uniq_name
    table_name = f"{stage_name}_t"
    stage_dir = tmp_path / stage_name
    stage_dir.mkdir()
    path = stage_dir / f"{format_name}_gbk.data"
    path.write_bytes(f"张三{delimiter}1\n".encode("gbk"))

    conn.exec(
        f"create or replace stage {stage_name} "
        f"url='{_stage_fs_url(stage_dir)}' "
        f"file_format=(type={format_name} encoding='gbk')"
    )

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
    strict_dir = tmp_path / strict_stage
    strict_dir.mkdir()
    path = strict_dir / f"{format_name}_mixed_utf8.data"
    path.write_bytes(b"ab\xffcd" + delimiter.encode() + b"1\n")

    conn.exec(
        f"create or replace stage {strict_stage} "
        f"url='{_stage_fs_url(strict_dir)}' "
        f"file_format=(type={format_name} encoding='utf8' encoding_error='strict')"
    )
    conn.exec(f"create or replace table {strict_table} (name string, id int)")

    with pytest.raises(Exception):
        conn.query_row(f"select $1 from @{strict_stage}")
    with pytest.raises(Exception):
        conn.query_row(f"copy into {strict_table} from @{strict_stage}")

    conn.exec(
        f"create or replace stage {replace_stage} "
        f"url='{_stage_fs_url(strict_dir)}' "
        f"file_format=(type={format_name} encoding='utf8' encoding_error='replace')"
    )
    conn.exec(f"create or replace table {replace_table} (name string, id int)")

    assert conn.query_row(f"select $1, $2 from @{replace_stage}").values() == (
        "ab\ufffdcd",
        "1",
    )

    res = conn.query_row(f"copy into {replace_table} from @{replace_stage}")
    assert res.values()[1] == 1
    assert conn.query_row(f"select * from {replace_table}").values() == ("ab\ufffdcd", 1)


@pytest.mark.parametrize(
    "format_name, delimiter",
    [("csv", ","), ("text", "\t")],
)
def test_copy_into_location_with_non_utf8_encoding(
    copy_env, tmp_path, format_name, delimiter
):
    conn = copy_env.conn
    output_path = tmp_path / f"{copy_env.uniq_name}_{format_name}_gbk.data"

    conn.exec(
        f"copy into 'fs://{_quote_sql_string(output_path)}' "
        "from (select '张三', 1) "
        f"file_format=(type={format_name} encoding='gbk') "
        "single=true use_raw_path=true overwrite=true"
    )

    assert output_path.read_bytes() == _expected_unload_bytes(format_name, delimiter)


@pytest.mark.parametrize(
    "format_name",
    ["csv", "text"],
)
def test_copy_into_location_with_unmappable_encoding(copy_env, tmp_path, format_name):
    conn = copy_env.conn
    output_path = tmp_path / f"{copy_env.uniq_name}_{format_name}_emoji.data"

    with pytest.raises(Exception):
        conn.exec(
            f"copy into 'fs://{_quote_sql_string(output_path)}' "
            "from (select '😀', 1) "
            f"file_format=(type={format_name} encoding='gbk') "
            "single=true use_raw_path=true overwrite=true"
        )

    assert not output_path.exists()
