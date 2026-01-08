import pytest
import databend_driver

from ..utils import DATABEND_DSL

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
    res = conn.query_row(f"copy into @{name} from (select '{val},1') file_format=(type=tsv)")
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
    res = conn.query_row(f"copy into @{name} from (select '{val},1') file_format=(type=tsv)")
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
    name = f"test_null_display_empty"
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
    res = conn.query_row(f"copy into @{name} from (select '{data},1') file_format=(type=tsv)")
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
    name = f"test_column_not_match"
    conn.exec(f"create or replace table {name} (a string, b string, c string)")
    conn.exec(f"create or replace stage {name}")

    # gen data
    res = conn.query_row(f"copy into @{name} from (select '{data}') file_format=(type=tsv)")
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
