// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_exception::ErrorCode;
use parquet::file::metadata::FileMetaData;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::metadata::ParquetMetaDataWriter;
use parquet::file::metadata::RowGroupMetaData;
use serde::Deserialize;

fn wrap_in_parquet_meta(row_group: &RowGroupMetaData) -> ParquetMetaData {
    let file_meta = FileMetaData::new(
        0,
        row_group.num_rows(),
        None,
        None,
        row_group.schema_descr_ptr(),
        None,
    );
    ParquetMetaData::new(file_meta, vec![row_group.clone()])
}

pub fn serialize_row_group_meta_to_bytes(meta: &RowGroupMetaData) -> Result<Vec<u8>, ErrorCode> {
    let parquet_meta = wrap_in_parquet_meta(meta);
    let mut buffer = Vec::new();
    ParquetMetaDataWriter::new(&mut buffer, &parquet_meta)
        .finish()
        .map_err(ErrorCode::from_std_error)?;
    Ok(buffer)
}

pub fn deserialize_row_group_meta_from_bytes(bytes: &[u8]) -> Result<RowGroupMetaData, ErrorCode> {
    let parquet_meta =
        ParquetMetaDataReader::decode_metadata(bytes).map_err(ErrorCode::from_std_error)?;
    parquet_meta
        .row_groups()
        .first()
        .cloned()
        .ok_or_else(|| ErrorCode::ParquetFileInvalid("serialized row group metadata is empty"))
}

pub fn serialize_row_group_meta<S>(
    meta: &RowGroupMetaData,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let bytes = serialize_row_group_meta_to_bytes(meta)
        .map_err(|e| serde::ser::Error::custom(e.to_string()))?;
    serializer.serialize_bytes(&bytes)
}

pub fn deserialize_row_group_meta<'de, D>(deserializer: D) -> Result<RowGroupMetaData, D::Error>
where D: serde::Deserializer<'de> {
    let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
    deserialize_row_group_meta_from_bytes(&bytes)
        .map_err(|e| serde::de::Error::custom(e.to_string()))
}
