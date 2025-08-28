import os
from databend_driver import BlockingDatabendClient

DATABEND_DSL = os.environ.get(
    "DATABEND_DSN", "databend://root:root@localhost:8000/?sslmode=disable"
)


def test_basic():

    client = BlockingDatabendClient(DATABEND_DSL)

    cursor = client.cursor()
    cursor.execute("SELECT * FROM numbers(10)")
    rows = cursor.fetchall()
    assert [row.values()[0] for row in rows] == [x for x in range(10)]
    cursor.close()
