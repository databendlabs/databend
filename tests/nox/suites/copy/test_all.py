import pytest

from .copy_utils import run_all


@pytest.mark.parametrize("fmt", ["ndjson", "text", "csv", "parquet"])
def test_sequence(copy_env, fmt):
    name = copy_env.uniq_name
    sqls = [
        # dest
        f'create or replace sequence {name};',
        f"create or replace table dest(seq int default nextval({name}), a int)",

        # use seq
        f"copy INTO @{name}/src1/ from (select number as a from numbers(2)) file_format=(type={fmt});",
        f"copy INTO dest(a) from @{name}/src1 file_format=(type={fmt}) return_failed_only=true;",
        # query
        (
            f"select seq, a from dest order by seq;",
            [(1,	0),
             (2,	1)]
        ),

        f"truncate table dest;",

        # not use seq
        f"copy INTO @{name}/src2/ from (select -number-1 as seq, number+1 as a from numbers(2)) file_format=(type={fmt});",
        f"copy INTO dest from @{name}/src2 file_format=(type={fmt}) return_failed_only=true;",
        (
            f"select seq, a from dest order by seq;",
            [(-2,	2),
             (-1,	1) ]
        ),
    ]

    run_all(copy_env.conn, sqls)
