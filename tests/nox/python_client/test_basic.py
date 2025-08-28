from databend_driver import BlockingDatabendClient
import importlib

from .utils import DATABEND_DSL


def test_version():
    # client = BlockingDatabendClient(DATABEND_DSL)
    # print(client.get_conn().version()) server version
    print(importlib.metadata.version("databend_driver"))


def test_basic_select():

    client = BlockingDatabendClient(DATABEND_DSL)

    cursor = client.cursor()
    cursor.execute("SELECT * FROM numbers(10)")
    rows = cursor.fetchall()
    assert [row.values()[0] for row in rows] == [x for x in range(10)]
    cursor.close()


def test_select_params():
    client = BlockingDatabendClient(DATABEND_DSL)

    conn = client.get_conn()
    # Test with positional parameters
    row = conn.query_row("SELECT ?, ?, ?, ?", (3, False, 4, "55"))
    assert row.values() == (3, False, 4, "55"), f"output: {row.values()}"

    # Test with named parameters
    row = conn.query_row(
        "SELECT :a, :b, :c, :d", {"a": 3, "b": False, "c": 4, "d": "55"}
    )
    assert row.values() == (3, False, 4, "55"), f"output: {row.values()}"

    row = conn.query_row("SELECT ?", 4)
    assert row.values() == (4,), f"output: {row.values()}"

    # Test with positional parameters again
    row = conn.query_row("SELECT ?, ?, ?, ?", (3, False, 4, "55"))
    assert row.values() == (3, False, 4, "55"), f"output: {row.values()}"
