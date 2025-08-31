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
