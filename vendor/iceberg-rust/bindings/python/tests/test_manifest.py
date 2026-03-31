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

from typing import Any, List
import pytest


from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
    _manifests,
)


@pytest.fixture(autouse=True)
def clear_global_manifests_cache() -> None:
    # Clear the global cache before each test
    _manifests.cache_clear()  # type: ignore


def fetch_manifest_entry(
    manifest_entry_file: str, io: FileIO, discard_deleted: bool = True
) -> List[ManifestEntry]:
    from pyiceberg_core import manifest

    bs = io.new_input(manifest_entry_file).open().read()
    manifest = manifest.read_manifest_entries(bs)

    # TODO: Don't convert the types
    # but this is the easiest for now until we
    # have the write part in there as well
    def _convert_entry(entry: Any) -> ManifestEntry:
        data_file = DataFile(
            DataFileContent(entry.data_file.content),
            entry.data_file.file_path,
            FileFormat(entry.data_file.file_format),
            [p.value() if p is not None else None for p in entry.data_file.partition],
            entry.data_file.record_count,
            entry.data_file.file_size_in_bytes,
            entry.data_file.column_sizes,
            entry.data_file.value_counts,
            entry.data_file.null_value_counts,
            entry.data_file.nan_value_counts,
            entry.data_file.lower_bounds,
            entry.data_file.upper_bounds,
            entry.data_file.key_metadata,
            entry.data_file.split_offsets,
            entry.data_file.equality_ids,
            entry.data_file.sort_order_id,
        )

        return ManifestEntry(
            ManifestEntryStatus(entry.status),
            entry.snapshot_id,
            entry.sequence_number,
            entry.file_sequence_number,
            data_file,
        )

    return [
        _convert_entry(entry)
        # _inherit_from_manifest(, self)
        for entry in manifest.entries()
        if not discard_deleted or entry.status != ManifestEntryStatus.DELETED
    ]


def test_read_manifest_entry(generated_manifest_entry_file: str) -> None:
    # manifest = ManifestFile.from_args(
    #     manifest_path=generated_manifest_entry_file,
    #     manifest_length=0,
    #     partition_spec_id=0,
    #     added_snapshot_id=0,
    #     sequence_number=0,
    #     partitions=[],
    # )
    # manifest_entries = manifest.fetch_manifest_entry(PyArrowFileIO())
    manifest_entries = fetch_manifest_entry(
        f"file://{generated_manifest_entry_file}", PyArrowFileIO()
    )
    manifest_entry = manifest_entries[0]

    assert manifest_entry.status == ManifestEntryStatus.ADDED
    assert manifest_entry.snapshot_id == 8744736658442914487
    assert manifest_entry.sequence_number == 0
    assert isinstance(manifest_entry.data_file, DataFile)

    data_file = manifest_entry.data_file

    assert data_file.content == DataFileContent.DATA
    assert (
        data_file.file_path
        == "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet"
    )
    assert data_file.file_format == FileFormat.PARQUET
    assert repr(data_file.partition) == "[1, None]"
    assert data_file.record_count == 19513
    assert data_file.file_size_in_bytes == 388872
    assert data_file.column_sizes == {
        1: 53,
        2: 98153,
    }
    assert data_file.value_counts == {
        1: 19513,
        2: 19513,
    }
    assert data_file.null_value_counts == {
        1: 19513,
        2: 0,
    }
    assert data_file.nan_value_counts == {}
    assert data_file.lower_bounds == {
        1: b"\x00\x00\x00\x00",
        2: b"\x00\x00\x00\x00\x00\x00\x00\x00",
    }
    assert data_file.upper_bounds == {
        1: b"\x00\x00\xe0\xbf",
        2: b"\x00\x00\x00\x00\x00\x00\xe0\xbf",
    }
    assert data_file.key_metadata is None
    assert data_file.split_offsets == [4]
    assert data_file.equality_ids is None
    assert data_file.sort_order_id == 0
