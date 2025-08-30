from databend_driver import BlockingDatabendClient

from .utils import DATABEND_DSL


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
