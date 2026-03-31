# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


from datetime import date, datetime
import uuid
import pytest
from pyiceberg_core.datafusion import IcebergDataFusionTable
from datafusion import SessionContext
from pyiceberg.catalog import Catalog, load_catalog
import pyarrow as pa
from pathlib import Path
import datafusion

assert (
    datafusion.__version__ >= "45"
)  # iceberg table provider only works for datafusion >= 45


@pytest.fixture(scope="session")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("warehouse")


@pytest.fixture(scope="session")
def catalog(warehouse: Path) -> Catalog:
    catalog = load_catalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse}",
        },
    )
    return catalog


@pytest.fixture(scope="session")
def arrow_table_with_null() -> "pa.Table":
    """Pyarrow table with all kinds of columns."""
    import pyarrow as pa

    return pa.Table.from_pydict(
        {
            "bool": [False, None, True],
            "string": ["a", None, "z"],
            # Go over the 16 bytes to kick in truncation
            "string_long": ["a" * 22, None, "z" * 22],
            "int": [1, None, 9],
            "long": [1, None, 9],
            "float": [0.0, None, 0.9],
            "double": [0.0, None, 0.9],
            # 'time': [1_000_000, None, 3_000_000],  # Example times: 1s, none, and 3s past midnight #Spark does not support time fields
            "timestamp": [
                datetime(2023, 1, 1, 19, 25, 00),
                None,
                datetime(2023, 3, 1, 19, 25, 00),
            ],
            # "timestamptz": [
            #     datetime(2023, 1, 1, 19, 25, 00, tzinfo=timezone.utc),
            #     None,
            #     datetime(2023, 3, 1, 19, 25, 00, tzinfo=timezone.utc),
            # ],
            "date": [date(2023, 1, 1), None, date(2023, 3, 1)],
            # Not supported by Spark
            # 'time': [time(1, 22, 0), None, time(19, 25, 0)],
            # Not natively supported by Arrow
            # 'uuid': [uuid.UUID('00000000-0000-0000-0000-000000000000').bytes, None, uuid.UUID('11111111-1111-1111-1111-111111111111').bytes],
            "binary": [b"\01", None, b"\22"],
            "fixed": [
                uuid.UUID("00000000-0000-0000-0000-000000000000").bytes,
                None,
                uuid.UUID("11111111-1111-1111-1111-111111111111").bytes,
            ],
        },
    )


def test_register_iceberg_table_provider(
    catalog: Catalog, arrow_table_with_null: pa.Table
) -> None:
    catalog.create_namespace_if_not_exists("default")
    iceberg_table = catalog.create_table_if_not_exists(
        "default.dataset",
        schema=arrow_table_with_null.schema,
    )
    iceberg_table.append(arrow_table_with_null)

    iceberg_table_provider = IcebergDataFusionTable(
        identifier=iceberg_table.name(),
        metadata_location=iceberg_table.metadata_location,
        file_io_properties=iceberg_table.io.properties,
    )

    ctx = SessionContext()
    ctx.register_table_provider("test", iceberg_table_provider)

    datafusion_table = ctx.table("test")
    assert datafusion_table is not None

    # check that the schema is the same
    from pyiceberg.io.pyarrow import _pyarrow_schema_ensure_small_types

    assert _pyarrow_schema_ensure_small_types(
        datafusion_table.schema()
    ) == _pyarrow_schema_ensure_small_types(iceberg_table.schema().as_arrow())
    # large/small type mismatches, fixed in pyiceberg 0.10.0
    # assert datafusion_table.schema() == iceberg_table.schema().as_arrow()

    # check that the data is the same
    assert (
        datafusion_table.to_arrow_table().to_pylist()
        == iceberg_table.scan().to_arrow().to_pylist()
    )
    # large/small type mismatches, fixed in pyiceberg 0.10.0
    # assert datafusion_table.to_arrow_table() == iceberg_table.scan().to_arrow()


def test_register_pyiceberg_table(
    catalog: Catalog, arrow_table_with_null: pa.Table
) -> None:
    from types import MethodType

    catalog.create_namespace_if_not_exists("default")
    iceberg_table = catalog.create_table_if_not_exists(
        "default.dataset",
        schema=arrow_table_with_null.schema,
    )
    iceberg_table.append(arrow_table_with_null)

    # monkey patch the __datafusion_table_provider__ method to the iceberg table
    def __datafusion_table_provider__(self):
        return IcebergDataFusionTable(
            identifier=self.name(),
            metadata_location=self.metadata_location,
            file_io_properties=self.io.properties,
        ).__datafusion_table_provider__()

    iceberg_table.__datafusion_table_provider__ = MethodType(
        __datafusion_table_provider__, iceberg_table
    )

    ctx = SessionContext()
    ctx.register_table_provider("test", iceberg_table)

    datafusion_table = ctx.table("test")
    assert datafusion_table is not None
