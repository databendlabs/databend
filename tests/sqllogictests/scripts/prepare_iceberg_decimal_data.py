"""Prepare mixed physical decimal encodings for the Iceberg reader regression test.

Requires the REST catalog and S3-compatible store from docker-compose-iceberg-tpch.yml.
The first file deliberately uses PyArrow's default FIXED_LEN_BYTE_ARRAY(7) for
DECIMAL(15,2); PyIceberg writes the second one as INT64.
"""

from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem
from pyiceberg.catalog import load_catalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import DecimalType, ListType, LongType, NestedField, StringType, StructType


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


def rows(keys, decimals):
    return pa.table(
        {
            "k": pa.array(keys, type=pa.int64()),
            "d": pa.array([Decimal(value) for value in decimals], type=pa.decimal128(15, 2)),
        }
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
    parquet_file = pq.ParquetFile(source)
    assert all(not field.metadata for field in parquet_file.schema_arrow)
    decimal = parquet_file.schema.column(1)
    assert decimal.physical_type == "FIXED_LEN_BYTE_ARRAY" and decimal.length == 7

table.add_files([location])

# PyIceberg writes precision <= 18 decimals as INT64. Both files must be read
# by the same Iceberg table, rather than planning from just one file's schema.
table.append(rows([4], ["42.00"]))

# Import ID-less files before two rounds of schema evolution. Historical aliases
# (including identical child names in different structs) must resolve by mapping,
# not by current names or by positions. Also read a new file with explicit IDs.
name = "test.t_name_mapping"
if catalog.table_exists(name):
    catalog.drop_table(name)
table = catalog.create_table(
    name,
    schema=Schema(
        NestedField(1, "old_k", LongType()),
        NestedField(2, "old_left", StructType(NestedField(3, "old_value", LongType()))),
        NestedField(4, "old_right", StructType(NestedField(5, "old_value", LongType()))),
        NestedField(6, "old_d", DecimalType(15, 2)),
    ),
)


def import_aliases(key, prefix, decimal):
    # Deliberately reverse file column order relative to the table schema.
    child = pa.struct([pa.field(f"{prefix}_value", pa.int64())])
    data = pa.table(
        {
            f"{prefix}_d": pa.array([Decimal(decimal)], pa.decimal128(15, 2)),
            f"{prefix}_right": pa.array([{f"{prefix}_value": key * 100}], child),
            f"{prefix}_left": pa.array([{f"{prefix}_value": key * 10}], child),
            f"{prefix}_k": pa.array([key], pa.int64()),
        }
    )
    location = f"{table.location().rstrip('/')}/data/{prefix}.parquet"
    parsed = urlparse(location)
    path = f"{parsed.netloc}{parsed.path}"
    with filesystem.open_output_stream(path) as output:
        pq.write_table(data, output)
    with filesystem.open_input_file(path) as source:
        arrow = pq.ParquetFile(source).schema_arrow
        assert all(not field.metadata for field in arrow)
    table.add_files([location])


import_aliases(1, "old", "123.45")
with table.update_schema() as update:
    update.rename_column("old_k", "legacy_k")
    update.rename_column("old_d", "legacy_d")
    update.rename_column("old_left.old_value", "legacy_value")
    update.rename_column("old_right.old_value", "legacy_value")
    update.rename_column("old_left", "legacy_left")
    update.rename_column("old_right", "legacy_right")

import_aliases(2, "legacy", "-7.01")
with table.update_schema() as update:
    update.rename_column("legacy_k", "k")
    update.rename_column("legacy_d", "d")
    update.rename_column("legacy_left.legacy_value", "value")
    update.rename_column("legacy_right.legacy_value", "value")
    update.rename_column("legacy_left", "left_item")
    update.rename_column("legacy_right", "right_item")
with table.update_schema() as update:
    update.move_first("d")
    update.move_first("right_item")

table.append(
    pa.table(
        {
            "right_item": pa.array([{"value": 300}], pa.struct([pa.field("value", pa.int64())])),
            "d": pa.array([Decimal("42.00")], pa.decimal128(15, 2)),
            "k": pa.array([3], pa.int64()),
            "left_item": pa.array([{"value": 30}], pa.struct([pa.field("value", pa.int64())])),
        }
    )
)
assert "schema.name-mapping.default" in table.properties

# Two tiny checked-in legacy two-level LIST files, written with the low-level
# Parquet writer (PyArrow writes three-level lists). Both contain k=[1,2,3] and
# old_items=[[{x:10},{x:20}],[{x:30}],NULL], with no IDs. Only the repeated group's
# physical name differs: `array` versus `old_items_tuple`.
for encoding in ["array", "tuple"]:
    name = f"test.t_legacy_list_{encoding}"
    if catalog.table_exists(name):
        catalog.drop_table(name)
    legacy = catalog.create_table(
        name,
        schema=Schema(
            NestedField(1, "k", LongType()),
            NestedField(2, "old_items", ListType(3, StructType(NestedField(4, "x", LongType())))),
        ),
    )
    source = Path(__file__).resolve().parents[2] / "data" / "parquet" / f"legacy_list_{encoding}.parquet"
    location = f"{legacy.location().rstrip('/')}/data/legacy.parquet"
    parsed = urlparse(location)
    with filesystem.open_output_stream(f"{parsed.netloc}{parsed.path}") as output:
        output.write(source.read_bytes())
    # PyIceberg add_files computes statistics assuming the three-level
    # `.list.element` path and rejects these legacy `.array`/`_tuple` paths.
    # Register the unchanged ID-less file through its manifest append API,
    # omitting optional column statistics and supplying the name mapping.
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=location,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=3,
        file_size_in_bytes=source.stat().st_size,
    )
    data_file.spec_id = legacy.spec().spec_id
    with legacy.transaction() as transaction:
        transaction.set_properties({
            "schema.name-mapping.default": legacy.schema().name_mapping.model_dump_json()
        })
        with transaction.update_snapshot().fast_append() as append:
            append.append_data_file(data_file)
    with legacy.update_schema() as update:
        update.rename_column("old_items.element.x", "y")
        update.rename_column("old_items", "items")

# A literal ':' in a column name is not an Iceberg path separator.
name = "test.t_colon_name"
if catalog.table_exists(name):
    catalog.drop_table(name)
colon = catalog.create_table(name, schema=Schema(NestedField(1, "a:b", LongType())))
colon.append(pa.table({"a:b": pa.array([1, 2, None], pa.int64())}))

# An unused malformed name mapping must not block files that already have IDs.
name = "test.t_unused_name_mapping"
if catalog.table_exists(name):
    catalog.drop_table(name)
identified = catalog.create_table(name, schema=Schema(NestedField(1, "k", LongType())))
identified.append(pa.table({"k": pa.array([1, 2], pa.int64())}))
with identified.transaction() as transaction:
    transaction.set_properties({"schema.name-mapping.default": "not json"})

# Rename after the last write: the current snapshot still references the old
# schema, so pushed-down select/filter must be translated by field ID. Each
# append is its own data file, letting a translated filter prune files.
# PyIceberg writes only counts (no bounds) for nested columns, so the third
# file's NULL `s.v` lets null counts prune on the renamed nested path.
name = "test.t_rename_after_write"
if catalog.table_exists(name):
    catalog.drop_table(name)
renamed = catalog.create_table(
    name,
    schema=Schema(
        NestedField(1, "k", LongType()),
        NestedField(2, "s", StructType(NestedField(3, "v", StringType()))),
    ),
)
renamed_child = pa.struct([pa.field("v", pa.string())])
for key, value in [(1, "a"), (2, "b"), (3, None)]:
    renamed.append(
        pa.table(
            {
                "k": pa.array([key], pa.int64()),
                "s": pa.array([{"v": value}], renamed_child),
            }
        )
    )
with renamed.update_schema() as update:
    update.rename_column("s.v", "w")
    update.rename_column("k", "id")
assert renamed.metadata.current_snapshot().schema_id != renamed.metadata.current_schema_id

# An ordinary stage file: no catalog properties, no field IDs and no Iceberg
# registration. It exercises the same prewhere/TopK policies in path mode.
filesystem.create_dir("iceberg-tpch/raw-parquet")
with filesystem.open_output_stream("iceberg-tpch/raw-parquet/data.parquet") as output:
    pq.write_table(
        pa.table({
            "v": pa.array([10, 20, 30], pa.int64()),
            "p": pa.array([False, True, True], pa.bool_()),
            "k": pa.array([1, 2, 3], pa.int64()),
        }),
        output,
        row_group_size=3,
    )

filesystem.create_dir("iceberg-tpch/raw-parquet-rich")
with filesystem.open_output_stream("iceberg-tpch/raw-parquet-rich/data.parquet") as output:
    pq.write_table(
        pa.table({
            "k": pa.array([1, 2, 3], pa.int64()),
            "obj": pa.array([{"value": 5}, None, {"value": 8}], pa.struct([pa.field("value", pa.int32())])),
            "arr": pa.array([[1, 2], None, []], pa.list_(pa.int32())),
            "m": pa.array([[('x', 10)], None, [('x', 30)]], pa.map_(pa.string(), pa.int32())),
            "d": pa.array([Decimal("123.45"), None, Decimal("-7.01")], pa.decimal128(15, 2)),
        }),
        output,
        row_group_size=3,
    )
