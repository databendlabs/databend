import pytest


def test_orc_unload_roundtrip(copy_env):
    conn = copy_env.conn
    name = copy_env.uniq_name

    conn.exec(
        f"copy into @{name}/orc_roundtrip "
        "from ("
        "select 1::int64 as id, 'alice' as name, true as active, "
        "1.5::float64 as score, to_binary('a1') as payload "
        "union all "
        "select 2::int64 as id, 'bob' as name, false as active, "
        "null::float64 as score, to_binary('b2') as payload"
        ") "
        "file_format=(type=orc) single=true use_raw_path=true overwrite=true"
    )

    rows = [
        row.values()
        for row in conn.query_iter(
            f"select id, name, active, score, payload "
            f"from @{name}/orc_roundtrip (file_format => 'orc') "
            "order by id"
        )
    ]

    assert rows == [
        (1, "alice", True, 1.5, b"a1"),
        (2, "bob", False, None, b"b2"),
    ]


def test_orc_unload_max_file_size(copy_env):
    conn = copy_env.conn
    name = copy_env.uniq_name
    num_rows = 1000000
    max_file_size = 20000000

    sql = (
        f"settings(max_threads=1) copy into @{name}/orc_size/ "
        "from ("
        "select number::int64 as id, repeat('x', 200) as payload "
        f"from numbers({num_rows})"
        ") "
        "file_format=(type=orc) "
        f"max_file_size={max_file_size} "
        "detailed_output=true"
    )

    rows = [row.values() for row in conn.query_iter(sql)]

    assert len(rows) == 8
    assert sum(row[2] for row in rows) == num_rows


def test_orc_unload_unsupported_type_reports_databend_type(copy_env):
    conn = copy_env.conn
    name = copy_env.uniq_name

    with pytest.raises(Exception, match="Databend data type UInt64"):
        conn.exec(
            f"copy into @{name}/orc_unsupported "
            "from (select 1::uint64 as id) "
            "file_format=(type=orc)"
        )
