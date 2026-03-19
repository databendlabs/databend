import re
import uuid
from dataclasses import dataclass

import databend_driver
import pytest

from ..utils import DATABEND_DSL
from .copy_utils import clean_up, prepare


@dataclass(frozen=True)
class CopyEnv:
    conn: object
    uniq_name: str


def _make_uniq_name(node_name):
    normalized = re.sub(r"[^0-9a-zA-Z_]+", "_", node_name).strip("_").lower()
    if not normalized or normalized[0].isdigit():
        normalized = f"test_{normalized}"

    return f"{normalized}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def conn():
    client = databend_driver.BlockingDatabendClient(DATABEND_DSL)
    return client.get_conn()


@pytest.fixture
def copy_env(conn, request):
    uniq_name = _make_uniq_name(request.node.name)
    prepare(conn, uniq_name)

    try:
        yield CopyEnv(conn=conn, uniq_name=uniq_name)
    finally:
        clean_up(conn, uniq_name)
