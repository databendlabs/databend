"""Prepare mixed physical decimal encodings for the Iceberg reader regression test.

Requires the REST catalog and S3-compatible store from docker-compose-iceberg-tpch.yml.
The first file deliberately uses PyArrow's default FIXED_LEN_BYTE_ARRAY(7) for
DECIMAL(15,2); PyIceberg writes the second one as INT64.
"""

from decimal import Decimal
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import DecimalType, LongType, NestedField


catalog = load_catalog(
    "iceberg_decimal_test",
    type="rest",
    uri="http://127.0.0.1:8181",
    warehouse="s3://iceberg-tpch/",
    **{
        "s3.endpoint": "http://127.0.0.1:9002",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    },
)
catalog.create_namespace_if_not_exists("test")
name = "test.t_decimal_physical"
# The preparation script can be rerun after a previous test run.
if catalog.table_exists(name):
    catalog.drop_table(name)

schema = Schema(
    NestedField(1, "k", LongType()),
    NestedField(2, "d", DecimalType(15, 2)),
)
table = catalog.create_table(name, schema=schema)
# Databend projects Iceberg columns by their Parquet field IDs, not by names.
# Use the catalog's schema so the manually written file carries those IDs too.
arrow_schema = schema_to_pyarrow(table.schema(), include_field_ids=True)


def rows(keys, decimals):
    return pa.table(
        {
            "k": pa.array(keys, type=pa.int64()),
            "d": pa.array(
                [Decimal(value) for value in decimals], type=pa.decimal128(15, 2)
            ),
        },
        schema=arrow_schema,
    )


filesystem = S3FileSystem(
    access_key="admin",
    secret_key="password",
    region="us-east-1",
    endpoint_override="127.0.0.1:9002",
    scheme="http",
)
location = f"{table.location().rstrip('/')}/data/decimal_flba.parquet"
parsed = urlparse(location)
path = f"{parsed.netloc}{parsed.path}"
with filesystem.open_output_stream(path) as output:
    pq.write_table(rows([1, 2, 3], ["123.45", "-7.01", "99999.99"]), output)
with filesystem.open_input_file(path) as source:
    parquet = pq.ParquetFile(source)
    for field in table.schema().fields:
        metadata = parquet.schema_arrow.field(field.name).metadata
        assert int(metadata[b"PARQUET:field_id"]) == field.field_id
    decimal = parquet.schema.column(1)
    assert decimal.physical_type == "FIXED_LEN_BYTE_ARRAY" and decimal.length == 7

table.add_files([location])

# PyIceberg writes precision <= 18 decimals as INT64. Both files must be read
# by the same Iceberg table, rather than planning from just one file's schema.
table.append(rows([4], ["42.00"]))
