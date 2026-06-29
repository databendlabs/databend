def test_avro_unload_max_file_size(copy_env):
    conn = copy_env.conn
    name = copy_env.uniq_name
    location = f"@{name}/avro_size/"

    num_rows = 100000
    max_file_size = 4096000
    sql_unload = (f"settings(max_threads=1) copy into {location} "
            "from ("
            "select number::int64 as id, repeat('x', 200) as payload "
            f"from numbers({num_rows})"
            ") "
            "file_format=(type=avro) "
            f"max_file_size={max_file_size} "
            "detailed_output=true")
    # print(sql_unload)
    rows = [row.values() for row in conn.query_iter(sql_unload)]
    # print(rows)
    assert len(rows) == 5
    assert sum(row[2] for row in rows) == num_rows
    assert max(row[1] for row in rows) < max_file_size * 1.1
