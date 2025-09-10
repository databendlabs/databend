from datetime import datetime, date, timedelta
from decimal import Decimal
from databend_driver import BlockingDatabendClient

from .utils import DATABEND_DSL


def test_data_types():
    client = BlockingDatabendClient(DATABEND_DSL)

    conn = client.get_conn()

    # Binary
    row = conn.query_row("select to_binary('xyz')")
    assert row.values() == (b"xyz",), f"Binary: {row.values()}"

    # Interval
    row = conn.query_row("select to_interval('1 microseconds')")
    assert row.values() == (timedelta(microseconds=1),), f"Interval: {row.values()}"

    # Decimal
    row = conn.query_row("SELECT 15.7563::Decimal(8,4), 2.0+3.0", params=[8, 4])
    assert row.values() == (
        Decimal("15.7563"),
        Decimal("5.0"),
    ), f"Decimal: {row.values()}"

    # Array
    row = conn.query_row("select [10::Decimal(15,2), 1.1+2.3]")
    assert row.values() == (
        [Decimal("10.00"), Decimal("3.40")],
    ), f"Array: {row.values()}"

    # Map
    row = conn.query_row("select {'xx':to_date('2020-01-01')}")
    assert row.values() == ({"xx": date(2020, 1, 1)},), f"Map: {row.values()}"

    # Tuple
    row = conn.query_row("select (10, '20', to_datetime('2024-04-16 12:34:56.789'))")
    assert row.values() == (
        (10, "20", datetime(2024, 4, 16, 12, 34, 56, 789000)),
    ), f"Tuple: {row.values()}"
