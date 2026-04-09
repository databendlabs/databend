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

use std::collections::BTreeMap;

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use parquet::file::metadata::ParquetMetaData;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexMeta {
    pub columns: Vec<(String, SingleColumnMeta)>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct IndexFile {
    pub name: String,
    pub data: Bytes,
}

impl IndexFile {
    pub fn create(name: String, data: Bytes) -> Self {
        Self { name, data }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SerializableIndexFile {
    name: String,
    data: Vec<u8>,
}

macro_rules! impl_bincode_codec_for_meta {
    ($ty:ty, $label:expr) => {
        impl TryFrom<&$ty> for Vec<u8> {
            type Error = ErrorCode;

            fn try_from(value: &$ty) -> std::result::Result<Self, Self::Error> {
                bincode::serde::encode_to_vec(value, bincode::config::standard()).map_err(|e| {
                    ErrorCode::StorageOther(format!("failed to encode {} meta {:?}", $label, e))
                })
            }
        }

        impl TryFrom<Bytes> for $ty {
            type Error = ErrorCode;

            fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
                bincode::serde::decode_from_slice(value.as_ref(), bincode::config::standard())
                    .map(|(v, len)| {
                        assert_eq!(len, value.len());
                        v
                    })
                    .map_err(|e| {
                        ErrorCode::StorageOther(format!("failed to decode {} meta {:?}", $label, e))
                    })
            }
        }
    };
}

macro_rules! impl_bincode_codec_for_file {
    ($ty:ty, $label:expr) => {
        impl TryFrom<&$ty> for Vec<u8> {
            type Error = ErrorCode;

            fn try_from(value: &$ty) -> std::result::Result<Self, Self::Error> {
                let serializable = SerializableIndexFile {
                    name: value.name.clone(),
                    data: value.data.to_vec(),
                };
                bincode::serde::encode_to_vec(&serializable, bincode::config::standard()).map_err(
                    |e| {
                        ErrorCode::StorageOther(format!("failed to encode {} file {:?}", $label, e))
                    },
                )
            }
        }

        impl TryFrom<Bytes> for $ty {
            type Error = ErrorCode;

            fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
                bincode::serde::decode_from_slice(value.as_ref(), bincode::config::standard())
                    .map(|(v, len)| {
                        assert_eq!(len, value.len());
                        v
                    })
                    .map(|v: SerializableIndexFile| IndexFile {
                        name: v.name,
                        data: v.data.into(),
                    })
                    .map_err(|e| {
                        ErrorCode::StorageOther(format!("failed to decode {} file {:?}", $label, e))
                    })
            }
        }
    };
}

impl_bincode_codec_for_meta!(IndexMeta, "index");
impl_bincode_codec_for_file!(IndexFile, "index");

pub(crate) fn index_columns_from_parquet_meta(
    meta: &ParquetMetaData,
) -> Vec<(String, SingleColumnMeta)> {
    let row_group = &meta.row_groups()[0];
    let mut col_metas = Vec::with_capacity(row_group.columns().len());
    for chunk_meta in row_group.columns() {
        let (offset, len) = chunk_meta.byte_range();
        let num_values = chunk_meta.num_values() as u64;
        let column_name = chunk_meta.column_path().parts()[0].to_owned();
        col_metas.push((column_name, SingleColumnMeta {
            offset,
            len,
            num_values,
        }));
    }
    col_metas.shrink_to_fit();
    col_metas
}

pub(crate) fn user_metadata_from_parquet_meta(meta: &ParquetMetaData) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    if let Some(key_value_metadata) = meta.file_metadata().key_value_metadata() {
        for key_value in key_value_metadata {
            let Some(value) = &key_value.value else {
                continue;
            };
            if key_value.key == "ARROW:schema" {
                continue;
            }
            metadata.insert(key_value.key.clone(), value.clone());
        }
    }
    metadata
}

impl TryFrom<ParquetMetaData> for IndexMeta {
    type Error = ErrorCode;

    fn try_from(meta: ParquetMetaData) -> std::result::Result<Self, Self::Error> {
        Ok(IndexMeta {
            columns: index_columns_from_parquet_meta(&meta),
            metadata: user_metadata_from_parquet_meta(&meta),
        })
    }
}
