from databend_driver import BlockingDatabendClient
from datetime import datetime, date

from .utils import DATABEND_DSL


def test_insert():
    client = BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()
    conn.exec(
        """
        CREATE OR REPLACE TABLE test (
            i64 Int64,
            u64 UInt64,
            f64 Float64,
            s   String,
            s2  String,
            d   Date,
            t   DateTime
        )
        """
    )
    conn.exec(
        r"""
        INSERT INTO test VALUES
            (-1, 1, 1.0, '\'', NULL, '2011-03-06', '2011-03-06 06:20:00'),
            (-2, 2, 2.0, '"', '', '2012-05-31', '2012-05-31 11:20:00'),
            (-3, 3, 3.0, '\\', 'NULL', '2016-04-04', '2016-04-04 11:30:00')
        """
    )
    rows = conn.query_iter("SELECT * FROM test")
    ret = [row.values() for row in rows]
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', "", date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"ret: {ret}"


def test_stream_load():
    client = BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()
    conn.exec(
        """
        CREATE OR REPLACE TABLE test (
            i64 Int64,
            u64 UInt64,
            f64 Float64,
            s   String,
            s2  String,
            d   Date,
            t   DateTime
        )
        """
    )
    values = [
        ["-1", "1", "1.0", "'", "\\N", "2011-03-06", "2011-03-06T06:20:00Z"],
        ["-2", "2", "2.0", '"', "", "2012-05-31", "2012-05-31T11:20:00Z"],
        ["-3", "3", "3.0", "\\", "NULL", "2016-04-04", "2016-04-04T11:30:00Z"],
    ]
    progress = conn.stream_load("INSERT INTO test VALUES", values)
    assert progress.write_rows == 3, f"progress.write_rows: {progress.write_rows}"
    assert progress.write_bytes == 194, f"progress.write_bytes: {progress.write_bytes}"

    rows = conn.query_iter("SELECT * FROM test")
    ret = [row.values() for row in rows]
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', None, date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"ret: {ret}"


def test_load_file_with_stage():
    run_load_file("stage")


def test_load_file_with_streaming():
    run_load_file("streaming")


def run_load_file(load_method):
    client = BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()
    conn.exec(
        """
        CREATE OR REPLACE TABLE test (
            i64 Int64,
            u64 UInt64,
            f64 Float64,
            s   String,
            s2  String,
            d   Date,
            t   DateTime
        )
        """
    )
    progress = conn.load_file(
        "INSERT INTO test VALUES FROM @_databend_load file_format = (type=csv)",
        "testdata/test.csv",
    )
    assert progress.write_rows == 3, (
        f"{load_method} progress.write_rows: {progress.write_rows}"
    )
    assert progress.write_bytes == 194, (
        f"{load_method}: progress.write_bytes: {progress.write_bytes}"
    )

    rows = conn.query_iter("SELECT * FROM test")
    ret = [row.values() for row in rows]
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', None, date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"{load_method} ret: {ret}"
