import pytest
import databend_driver

from ..utils import DATABEND_DSL
from .copy_utils import unload_and_read_stage_text

test_empty_data = [
    # test empty_field_as
    (1, '', "VARCHAR default 'x'", ("null", "string"), (None, None)),
    (2, '', "VARCHAR default 'x'", ("string", "string"), ('', '')),
    (3, '', "VARCHAR default 'x'", ("field_default", "string"), ('x', None)),

    # load into field not null
    (4, '', "int not null", ("null", "string"), ("err", None)),
    (5, '', "int not null", ("string", "string"), ("err", '')),
    (6, '', "int not null", ("field_default", "string"), (0, None)),

    # test quoted empty behavior
    (7, '""', "VARCHAR", ("null", "string"), ('', '')),
    (8, '""', "VARCHAR", ("null", "null"), (None, None)),
]

@pytest.mark.parametrize("tid, val, typ, options, expected", test_empty_data)
def test_empty(tid, val, typ, options, expected):
    (empty_field_as, quoted_empty_field_as) = options
    (expected_copy, expected_select) = expected

    client = databend_driver.BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()
    name = f"test_empty_{tid}x"
    fmt = (
        "file_format=(type=csv, EMPTY_FIELD_AS={empty_field_as} "
        "QUOTED_EMPTY_FIELD_AS={quoted_empty_field_as})"
    ).format(empty_field_as=empty_field_as, quoted_empty_field_as=quoted_empty_field_as)
    conn.exec(f"create or replace table {name} (a {typ}, b int)")
    conn.exec(f"create or replace stage {name} {fmt}")

    # gen data
    res = conn.query_row(f"copy into @{name} from (select '{val},1') file_format=(type=TSV)")
    assert res.values()[0] == 1

    # test copy
    copy_sql = f"copy into {name} from @{name}"
    err = None
    res = None
    try:
        res = conn.query_row(copy_sql)
    except Exception as e:
        err = e
    if expected_copy == "err":
        assert err is not None
    else:
        assert res.values()[1] == 1
        res = conn.query_row(f"select * from {name}")
        assert res.values()[0] == expected_copy

    # test select
    select_sql = f"select $1 from @{name}"
    res = conn.query_row(select_sql)
    assert res.values()[0] == expected_select


test_null_data = [
    (1, 'NUL', "VARCHAR default 'x'", ("NUL", False), (None, None)),
    (3, 'NUL', "int not null", ("NUL", True), ("err", None)),
    (2, '"NUL"', "VARCHAR default 'x'", ("NUL", False), ("NUL", "NUL")),
    (3, '"NUL"', "VARCHAR default 'x'", ("NUL", True), (None, None)),
]

@pytest.mark.parametrize("tid, val, typ, options, expected", test_null_data)
def test_null(tid, val, typ, options, expected):
    (null_display, allow_quoted_nulls) = options
    (expected_copy, expected_select) = expected

    client = databend_driver.BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()
    name = f"test_null_{tid}x"
    fmt = f"file_format=(type=csv, null_display='{null_display}' allow_quoted_nulls={allow_quoted_nulls})"
    conn.exec(f"create or replace table {name} (a {typ}, b int)")
    conn.exec(f"create or replace stage {name} {fmt}")

    # gen data
    res = conn.query_row(f"copy into @{name} from (select '{val},1') file_format=(type=TSV)")
    assert res.values()[0] == 1

    # test copy
    copy_sql = f"copy into {name} from @{name}"
    err = None
    res = None
    try:
        res = conn.query_row(copy_sql)
    except Exception as e:
        err = e
    if expected_copy == "err":
        assert err is not None
    else:
        assert res.values()[1] == 1
        res = conn.query_row(f"select * from {name}")
        assert res.values()[0] == expected_copy

    # test select
    select_sql = f"select $1 from @{name}"
    res = conn.query_row(select_sql)
    assert res.values()[0] == expected_select

@pytest.mark.parametrize("data", ['', '""'])
@pytest.mark.parametrize("allow_quoted_nulls", [False, True])
@pytest.mark.parametrize("quoted_empty_field_as", ["string", "null"])
@pytest.mark.parametrize("empty_field_as", ["null", "string"])
def test_null_display_empty(
    data, allow_quoted_nulls, quoted_empty_field_as, empty_field_as
):
    client = databend_driver.BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()
    name = "test_null_display_empty"
    fmt = (
        "file_format=(type=csv, null_display='' empty_field_as={empty_field_as} "
        "allow_quoted_nulls={allow_quoted_nulls} QUOTED_EMPTY_FIELD_AS={quoted_empty_field_as})"
    ).format(
        empty_field_as=empty_field_as,
        allow_quoted_nulls=allow_quoted_nulls,
        quoted_empty_field_as=quoted_empty_field_as,
    )
    conn.exec(f"create or replace table {name} (a string, b int)")
    conn.exec(f"create or replace stage {name} {fmt}")

    # gen data
    res = conn.query_row(f"copy into @{name} from (select '{data},1') file_format=(type=TSV)")
    assert res.values()[0] == 1

    # test
    select_sql = f"select $1 from @{name}"
    res = conn.query_row(select_sql)

    exp = None
    # not affected by allow_quoted_nulls
    if data == "":
        if empty_field_as == "string":
            exp = ""
    else:
        if quoted_empty_field_as == "string":
            exp = ""

    assert res.values()[0] == exp


test_column_not_match_data = [
    ("1,2,3", True, 1),
    ("2,2,3,", True, 1),
    ('3,2,3,""', True, 0),
    ('4,2,3, ', True, 0),
    ("5,2", True, 0),
    ("6,2", False, 1),
    ("7,2,3,4", False, 1),
]

@pytest.mark.parametrize("data,check,exp", test_column_not_match_data)
def test_column_not_match(data, check, exp):
    client = databend_driver.BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()
    name = "test_column_not_match"
    conn.exec(f"create or replace table {name} (a string, b string, c string)")
    conn.exec(f"create or replace stage {name}")

    # gen data
    res = conn.query_row(f"copy into @{name} from (select '{data}') file_format=(type=TSV)")
    assert res.values()[0] == 1

    copy_sql = f"copy into {name} from @{name} file_format=(type=csv error_on_column_count_mismatch={check}) on_error=continue"
    select_sql = f"select count($1) from {name}"

    res = conn.query_row(copy_sql)
    print(res.values())
    # row loaded
    assert res.values()[1] == exp

    if not exp:
        assert "Number of columns" in res.values()[3]

    res = conn.query_row(select_sql)
    assert res.values()[0] == exp


def test_trim_space(copy_env):
    conn = copy_env.conn
    name = copy_env.uniq_name
    path = f"@{name}/trim_space.csv"

    content = unload_and_read_stage_text(
        copy_env,
        path,
        """(select ' 42 ,"  hello  " ,  NULL  ')""",
        "file_format=(type=TSV)",
    )
    assert content == ' 42 ,"  hello  " ,  NULL  \n'

    conn.exec("create or replace table t_trim_csv (a int, b string, c string null)")
    res = conn.query_row(
        f"copy into t_trim_csv from {path} "
        "file_format=(type=csv trim_space=true null_display='NULL')"
    )
    assert res.values()[1] == 1
    assert conn.query_row("select * from t_trim_csv").values() == (42, "hello", None)


def _assert_csv_roundtrip(conn, path, file_format):
    conn.exec("create table reloaded like source")
    conn.exec(f"copy into reloaded from {path} {file_format}")
    assert (
        conn.query_row("select count(*) from reloaded").values()
        == conn.query_row("select count(*) from source").values()
    )
    assert conn.query_row(
        "select count(*) from (select * from source except select * from reloaded)"
    ).values() == (0,)


@pytest.mark.parametrize(
    "quote_style, expected",
    [
        (
            "quote_not_null",
            '"1","1.2","1.5","true","FF","123","2026-01-02","[1]","123"\n',
        ),
        ("quote_non_numeric", '1,1.2,1.5,"true","FF","123","2026-01-02","[1]","123"\n'),
        ("quote_minimal", "1,1.2,1.5,true,FF,123,2026-01-02,[1],123\n"),
    ],
    ids=["not-null", "non-numeric", "minimal"],
)
@pytest.mark.parametrize("format_source", ["inline", "named"])
def test_quote_styles(copy_env, quote_style, expected, format_source):
    conn = copy_env.conn
    conn.exec(
        "create table source (n int, d decimal(10,2), f double, b boolean, "
        "x binary, s string, day date, a array(int), v variant)"
    )
    conn.exec(
        "insert into source values "
        "(1, 1.20, 1.5, true, from_hex('ff'), '123', '2026-01-02', [1], parse_json('123')), "
        "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    )
    fmt = f"file_format=(type=csv quote_style='{quote_style}')"
    format_name = f"csv_style_{copy_env.uniq_name}"
    if format_source == "named":
        conn.exec(f"create file format {format_name} type=csv quote_style={quote_style}")
        fmt = f"file_format=(format_name='{format_name}')"
    try:
        path = f"@{copy_env.uniq_name}/styles.csv"
        content = unload_and_read_stage_text(
            copy_env, path, "(select * from source order by n nulls last)", fmt
        )
        assert content == expected + "\\N,\\N,\\N,\\N,\\N,\\N,\\N,\\N,\\N\n"
        _assert_csv_roundtrip(conn, path, fmt)
    finally:
        if format_source == "named":
            conn.exec(f"drop file format {format_name}")


@pytest.mark.parametrize(
    "quote_style, expected",
    [
        (
            "quote_not_null",
            '"1","","","",""\n"2",{null},{null},"",""\n'
            '"3","{binary}","x","{binary}","x"\n',
        ),
        (
            "quote_non_numeric",
            '1,"","","",""\n2,{null},{null},"",""\n3,"{binary}","x","{binary}","x"\n',
        ),
        (
            "quote_minimal",
            '1,"","","",""\n2,{null},{null},"",""\n3,{binary},x,{binary},x\n',
        ),
    ],
    ids=["not-null", "non-numeric", "minimal"],
)
@pytest.mark.parametrize("binary_format, binary", [("hex", "FF"), ("base64", "/w==")])
@pytest.mark.parametrize("quote", ['"', "'", "`"], ids=["double", "single", "backtick"])
@pytest.mark.parametrize(
    "null_display, null_option",
    [
        (r"\N", ""),
        ("", "null_display=''"),
    ],
    ids=["default-null", "empty-null"],
)
def test_empty_roundtrip(
    copy_env,
    quote_style,
    expected,
    binary_format,
    binary,
    quote,
    null_display,
    null_option,
):
    """Empty nullable/NOT NULL STRING and BINARY must stay distinct from NULL (#20773)."""
    conn = copy_env.conn
    conn.exec(
        "create table source (id int, x binary null, s string null, "
        "required_x binary not null, required_s string not null)"
    )
    conn.exec(
        "insert into source values "
        "(1, from_hex(''), '', from_hex(''), ''), "
        "(2, NULL, NULL, from_hex(''), ''), "
        "(3, from_hex('ff'), 'x', from_hex('ff'), 'x')"
    )
    sql_quote = quote.replace("'", "''")
    fmt = (
        f"file_format=(type=csv quote_style='{quote_style}' "
        f"binary_format={binary_format} quote='{sql_quote}' {null_option})"
    )
    path = f"@{copy_env.uniq_name}/empty.csv"
    content = unload_and_read_stage_text(
        copy_env, path, "(select * from source order by id)", fmt
    )
    # These explicit templates only substitute the quote, binary encoding, and marker.
    assert content == expected.format(binary=binary, null=null_display).replace(
        '"', quote
    )
    _assert_csv_roundtrip(conn, path, fmt)


@pytest.mark.parametrize(
    "quote_style", ["quote_not_null", "quote_non_numeric", "quote_minimal"]
)
@pytest.mark.parametrize(
    "expression, marker, binary_format",
    [
        ("123::int", "123", "hex"),
        ("1.5::double", "1.5", "hex"),
        ("1.20::decimal(10,2)", "1.2", "hex"),
        ("true", "true", "hex"),
        ("'123'", "123", "hex"),
        (r"'\\N'", r"\N", "hex"),
        ("'NULL'", "NULL", "hex"),
        ("to_date('1970-01-01')", "1970-01-01", "hex"),
        ("from_hex('ff')", "FF", "hex"),
        ("from_hex('ff')", "/w==", "base64"),
    ],
)
def test_quote_null_marker(copy_env, quote_style, expression, marker, binary_format):
    conn = copy_env.conn
    conn.exec(f"create table source as select {expression} as value, null::int as n")
    sql_marker = marker.replace("\\", "\\\\").replace("'", "''")
    fmt = (
        f"file_format=(type=csv quote_style='{quote_style}' "
        f"null_display='{sql_marker}' binary_format={binary_format})"
    )
    path = f"@{copy_env.uniq_name}/collision.csv"
    assert (
        unload_and_read_stage_text(copy_env, path, "source", fmt)
        == f'"{marker}",{marker}\n'
    )
    _assert_csv_roundtrip(conn, path, fmt)


@pytest.mark.parametrize(
    "quote_style, expected",
    [
        ("quote_not_null", '"[]","{}"\n'),
        ("quote_non_numeric", '"[]","{}"\n'),
        ("quote_minimal", "[],{}\n"),
    ],
    ids=["not-null", "non-numeric", "minimal"],
)
def test_quote_empty_containers(copy_env, quote_style, expected):
    assert (
        unload_and_read_stage_text(
            copy_env,
            f"@{copy_env.uniq_name}/containers.csv",
            "(select [], {})",
            f"file_format=(type=csv quote_style='{quote_style}')",
        )
        == expected
    )


@pytest.mark.parametrize(
    "quote_style", ["quote_not_null", "quote_non_numeric", "quote_minimal"]
)
def test_quote_numeric_delimiters(copy_env, quote_style):
    conn = copy_env.conn
    conn.exec("create table source (n double null)")
    conn.exec("insert into source values (1.5), (NULL)")
    fmt = (
        f"file_format=(type=csv quote_style='{quote_style}' "
        "field_delimiter='.' record_delimiter=';' quote='`')"
    )
    path = f"@{copy_env.uniq_name}/numeric.csv"
    assert (
        unload_and_read_stage_text(
            copy_env, path, "(select * from source order by n nulls last)", fmt
        )
        == "`1.5`;\\N;"
    )
    _assert_csv_roundtrip(conn, path, fmt)
