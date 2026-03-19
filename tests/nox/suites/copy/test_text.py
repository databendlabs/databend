import pytest

from .copy_utils import run_all


@pytest.mark.parametrize("type_name", ["tsv", "text"])
def test_text_alias(copy_env, type_name):
    sqls = [
        # create file format
        f'create or replace file format my_{type_name} type={type_name};',

        # unload
        f"copy into @{copy_env.uniq_name}/ from (select 1, 2) file_format=(type={type_name});",

        # load
        "create or replace table t1 (a int, b int);",
        f"copy into t1 from @{copy_env.uniq_name}/ file_format=(type={type_name});",
        ("select * from t1;", (1, 2)),

        # query
        (
            f"select $1, $2 from @{copy_env.uniq_name} (file_format=>'{type_name}');",
            ("1", "2"),
        ),
    ]

    run_all(copy_env.conn, sqls)
