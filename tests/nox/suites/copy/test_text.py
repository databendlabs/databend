import pytest

from .copy_utils import unload_and_read_stage_text
from .copy_utils import run_all


def _quote_sql_string(value):
    value = str(value)
    return value.replace("'", "''")


def _stage_fs_url(path):
    return f"fs://{_quote_sql_string(path)}/"


@pytest.mark.parametrize("type_name", ["tsv", "text"])
def test_text_alias(copy_env, type_name):
    name = copy_env.uniq_name
    sqls = [
        # create file format
        f'create or replace file format my_{type_name} type={type_name};',

        # unload
        f"copy into @{name}/ from (select 1, 2) file_format=(type={type_name});",
        (f"select right(name, 4)  from list_stage(location=>'@{name}')", (".txt",)),

        # load
        "create or replace table t1 (a int, b int);",
        f"copy into t1 from @{name}/ file_format=(type={type_name});",
        ("select * from t1;", (1, 2)),

        # query
        (
            f"select $1, $2 from @{name} (file_format=>'{type_name}');",
            ("1", "2"),
        ),
    ]

    run_all(copy_env.conn, sqls)


def _copy_into_table(conn, table, location, file_format_clause, on_error="continue"):
    sql = f"copy into {table} from {location} {file_format_clause} on_error={on_error}"
    return conn.query_row(sql).values()


def test_text_output_header(copy_env):
    conn = copy_env.conn
    name = copy_env.uniq_name
    path = f"@{name}/with_header.txt"

    content = unload_and_read_stage_text(
        copy_env,
        path,
        "(select 1 as a, 2 as b)",
        "file_format=(type=text output_header=true)",
    )
    assert content == "a\tb\n1\t2\n"

    conn.exec("create or replace table t_header (a int, b int null)")
    res = conn.query_row(
        f"copy into t_header from {path} file_format=(type=text skip_header=1)"
    )
    assert res.values()[1] == 1
    assert conn.query_row("select * from t_header").values() == (1, 2)


def test_text_null_display(copy_env):
    conn = copy_env.conn
    name = copy_env.uniq_name
    path = f"@{name}/null_display.txt"

    content = unload_and_read_stage_text(
        copy_env,
        path,
        "(select null as a, 1 as b)",
        "file_format=(type=text null_display='NULL')",
    )
    assert content == "NULL\t1\n"

    conn.exec("create or replace table t_null_display (a string null, b int)")
    res = conn.query_row(
        f"copy into t_null_display from {path} "
        "file_format=(type=text null_display='NULL')"
    )
    assert res.values()[1] == 1
    assert conn.query_row("select * from t_null_display").values() == (None, 1)


@pytest.mark.parametrize(
    "field_delimiter_clause",
    ["field_delimiter=''", "field_delimiter='|'"],
    ids=["line_mode", "delimited"],
)
@pytest.mark.parametrize(
    "error_on_column_count_mismatch",
    [True, False],
    ids=["strict", "lenient"],
)
def test_text_missing_columns_matrix(
    copy_env, field_delimiter_clause, error_on_column_count_mismatch
):
    conn = copy_env.conn
    name = copy_env.uniq_name
    delimiter_name = field_delimiter_clause.replace("'", "").replace("=", "_")
    path = (
        f"@{name}/missing_"
        f"{delimiter_name}_"
        f"{str(error_on_column_count_mismatch).lower()}.txt"
    )

    #It is not supported to unload TEXT file when FIELD_DELIMITER is ''
    content = unload_and_read_stage_text(
        copy_env,
        path,
        "(select 'alpha' as a)",
        f"file_format=(type=text)",
    )
    assert content == "alpha\n"

    conn.exec(
        "create or replace table t_missing (a string, b string default 'fallback')"
    )
    res = _copy_into_table(
        conn,
        "t_missing",
        path,
        "file_format=("
        f"type=text {field_delimiter_clause} "
        f"error_on_column_count_mismatch={str(error_on_column_count_mismatch).lower()} "
        "empty_field_as=field_default"
        ")",
    )

    if error_on_column_count_mismatch:
        assert res[1] == 0
        assert "Number of columns" in res[3]
    else:
        assert res[1] == 1
        assert conn.query_row("select * from t_missing").values() == (
            "alpha",
            "fallback",
        )


@pytest.mark.parametrize(
    "error_on_column_count_mismatch, expected_loaded, expected_row",
    [
        (True, 0, None),
        (False, 1, ("alpha", "beta")),
    ],
    ids=["strict", "lenient"],
)
def test_text_extra_columns(copy_env, error_on_column_count_mismatch, expected_loaded, expected_row):
    conn = copy_env.conn
    name = copy_env.uniq_name
    path = f"@{name}/extra_columns.txt"

    content = unload_and_read_stage_text(
        copy_env,
        path,
        "(select 'alpha' as a, 'beta' as b, 'gamma' as c)",
        "file_format=(type=text field_delimiter='|')",
    )
    assert content == "alpha|beta|gamma\n"

    conn.exec("create or replace table t_extra (a string, b string)")
    res = _copy_into_table(
        conn,
        "t_extra",
        path,
        "file_format=("
        "type=text field_delimiter='|' "
        f"error_on_column_count_mismatch={str(error_on_column_count_mismatch).lower()}"
        ")",
    )

    assert res[1] == expected_loaded
    if expected_row is None:
        assert "Number of columns" in res[3]
    else:
        assert conn.query_row("select * from t_extra").values() == expected_row


@pytest.mark.parametrize(
    "tid, column_type, empty_field_as, expected",
    [
        (1, "string default 'x'", "field_default", ("ok", "x")),
        (2, "string default 'x'", "string", ("ok", "")),
        (3, "string null", "string", ("ok", "")),
        (4, "string null", "null", ("ok", None)),
        (5, "int not null", "null", ("err", None)),
        (6, "int not null", "string", ("err", None)),
    ],
)
def test_text_empty_field_as(copy_env, tid, column_type, empty_field_as, expected):
    conn = copy_env.conn
    name = copy_env.uniq_name
    path = f"@{name}/empty_field_as_{tid}.txt"

    content = unload_and_read_stage_text(
        copy_env,
        path,
        "(select '' as a, 1 as b)",
        "file_format=(type=text field_delimiter='|')",
    )
    assert content == "|1\n"

    conn.exec(f"create or replace table t_empty_{tid} (a {column_type}, b int)")
    sql = (
        f"copy into t_empty_{tid} from {path} "
        "file_format=(type=text field_delimiter='|' "
        f"empty_field_as={empty_field_as})"
    )

    if expected[0] == "err":
        with pytest.raises(Exception):
            conn.query_row(sql)
    else:
        res = conn.query_row(sql)
        assert res.values()[1] == 1
        assert conn.query_row(f"select * from t_empty_{tid}").values() == (
            expected[1],
            1,
        )


def test_text_trim_space(copy_env):
    conn = copy_env.conn
    name = copy_env.uniq_name
    path = f"@{name}/trim_space.txt"

    content = unload_and_read_stage_text(
        copy_env,
        path,
        """(select ' 42 |  hello  |  NULL  ')""",
        "file_format=(type=TSV)",
    )
    assert content == " 42 |  hello  |  NULL  \n"

    conn.exec("create or replace table t_trim_text (a int, b string, c string null)")
    res = conn.query_row(
        f"copy into t_trim_text from {path} "
        "file_format=(type=text field_delimiter='|' trim_space=true null_display='NULL')"
    )
    assert res.values()[1] == 1
    assert conn.query_row("select * from t_trim_text").values() == (42, "hello", None)


def test_text_trim_space_after_escape_decode(copy_env, tmp_path):
    conn = copy_env.conn
    stage_name = copy_env.uniq_name
    table_name = f"{stage_name}_t"
    stage_dir = tmp_path / stage_name
    stage_dir.mkdir()
    path = stage_dir / "trim_escaped_space.txt"
    path.write_bytes(b"abc\\ \n")

    conn.exec(
        f"create or replace stage {stage_name} "
        f"url='{_stage_fs_url(stage_dir)}' "
        "file_format=(type=text trim_space=true)"
    )
    conn.exec(f"create or replace table {table_name} (a string)")

    res = conn.query_row(f"copy into {table_name} from @{stage_name}")
    assert res.values()[1] == 1
    assert conn.query_row(f"select * from {table_name}").values() == ("abc\\",)
