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

use std::collections::HashMap;

use arrow_schema::Schema;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::types::DataType;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use parquet::format::FileMetaData;

pub const VIRTUAL_COLUMN_STRING_TABLE_KEY: &str = "virtual_column_string_table";
pub const VIRTUAL_COLUMN_NODES_KEY: &str = "virtual_column_nodes";
pub const VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY: &str = "virtual_column_shared_column_ids";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum VirtualColumnNameIndex {
    // column_metas index
    Column(u32),
    // shared column index, index is shared map key index
    Shared(u32),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct VirtualColumnNode {
    // children: segment_id -> child node, where segment_id references string_table.
    pub children: HashMap<u32, VirtualColumnNode>,
    // leaf: terminal node that maps to a virtual column or shared map entry.
    pub leaf: Option<VirtualColumnNameIndex>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct VirtualColumnIdWithMeta {
    pub column_id: u32,
    pub meta: SingleColumnMeta,
    pub data_type: DataType,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct VirtualColumnFileMeta {
    // All unique path segment strings in this file.
    pub string_table: Vec<String>,
    // Column metadata for all virtual columns (offset/len/num_values + type + column id).
    pub column_metas: Vec<VirtualColumnIdWithMeta>,
    // Sparse paths are stored in a shared map column: source_id -> (key column, value column).
    pub shared_column_metas: HashMap<u32, (VirtualColumnIdWithMeta, VirtualColumnIdWithMeta)>,
    // Per source column trie: source_column_id -> root node.
    pub virtual_column_nodes: HashMap<u32, VirtualColumnNode>,
}

impl TryFrom<&VirtualColumnFileMeta> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &VirtualColumnFileMeta) -> std::result::Result<Self, Self::Error> {
        bincode::serde::encode_to_vec(value, bincode::config::standard()).map_err(|e| {
            ErrorCode::StorageOther(format!("failed to encode vector index meta {:?}", e))
        })
    }
}

impl TryFrom<Bytes> for VirtualColumnFileMeta {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::serde::decode_from_slice(value.as_ref(), bincode::config::standard())
            .map(|(v, len)| {
                assert_eq!(len, value.len());
                v
            })
            .map_err(|e| {
                ErrorCode::StorageOther(format!("failed to decode vector index meta {:?}", e))
            })
    }
}

// Virtual columns are extracted from Variant/JSON values into independent parquet columns.
// Each source column builds a trie keyed by path segments; leaf nodes point to either a
// dedicated virtual column or a shared map entry for sparse paths. The parquet file stores:
// - ARROW:schema: column names/types for all virtual columns.
// - VIRTUAL_COLUMN_STRING_TABLE_KEY: a string table of unique path segments.
// - VIRTUAL_COLUMN_NODES_KEY: the trie encoded by string table ids and leaf indices.
// - VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY: mapping of source_column_id -> (key_id, value_id).
// Column offsets/lengths/num_values and shared map columns are derived from parquet metadata.
impl TryFrom<FileMetaData> for VirtualColumnFileMeta {
    type Error = ErrorCode;

    fn try_from(mut meta: FileMetaData) -> std::result::Result<Self, Self::Error> {
        let mut arrow_schema = None;
        let mut string_table = None;
        let mut virtual_column_nodes = None;
        let mut shared_column_ids = None;

        let key_value_metadata = meta.key_value_metadata.unwrap_or_default();
        for key_value in &key_value_metadata {
            if key_value.key == "ARROW:schema" {
                let encoded_meta = key_value.value.as_ref().unwrap();
                arrow_schema = Some(get_arrow_schema_from_metadata(encoded_meta)?);
            } else if key_value.key == VIRTUAL_COLUMN_STRING_TABLE_KEY {
                let encoded_meta = key_value.value.as_ref().unwrap();
                string_table = Some(get_virtual_column_string_table(encoded_meta)?);
            } else if key_value.key == VIRTUAL_COLUMN_NODES_KEY {
                let encoded_meta = key_value.value.as_ref().unwrap();
                virtual_column_nodes = Some(get_virtual_column_nodes(encoded_meta)?);
            } else if key_value.key == VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY {
                let encoded_meta = key_value.value.as_ref().unwrap();
                shared_column_ids = Some(get_virtual_column_shared_ids(encoded_meta)?);
            }
        }

        let arrow_schema = arrow_schema.ok_or_else(|| {
            ErrorCode::Internal("virtual column file missing ARROW:schema metadata")
        })?;
        let data_schema = DataSchema::try_from(&arrow_schema)?;
        let string_table = string_table.ok_or_else(|| {
            ErrorCode::Internal("virtual column file missing virtual column string table metadata")
        })?;
        let virtual_column_nodes = virtual_column_nodes.ok_or_else(|| {
            ErrorCode::Internal("virtual column file missing virtual column nodes metadata")
        })?;
        let shared_column_ids = shared_column_ids.unwrap_or_default();

        let rg = meta.row_groups.remove(0);
        let mut column_metas = Vec::with_capacity(rg.columns.len());
        for (column_idx, column) in rg.columns.iter().enumerate() {
            let chunk_meta = column.meta_data.as_ref().ok_or_else(|| {
                ErrorCode::Internal(
                    "expecting chunk meta data while converting ThriftFileMetaData to VirtualColumnFileMeta",
                )
            })?;
            let col_start = if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
                dict_page_offset
            } else {
                chunk_meta.data_page_offset
            };
            let col_len = chunk_meta.total_compressed_size;
            assert!(
                col_start >= 0 && col_len >= 0,
                "column start and length should not be negative"
            );
            let num_values = chunk_meta.num_values as u64;
            let res = SingleColumnMeta {
                offset: col_start as u64,
                len: col_len as u64,
                num_values,
            };
            let data_type = data_type_from_path(&data_schema, &chunk_meta.path_in_schema)?;
            let column_id = column_idx as u32;
            column_metas.push(VirtualColumnIdWithMeta {
                column_id,
                meta: res,
                data_type,
            });
        }

        let mut shared_column_metas = HashMap::new();
        for (source_id, (key_idx, value_idx)) in shared_column_ids {
            let Some(key_meta) = column_metas.get(key_idx as usize) else {
                continue;
            };
            let Some(value_meta) = column_metas.get(value_idx as usize) else {
                continue;
            };
            shared_column_metas.insert(source_id, (key_meta.clone(), value_meta.clone()));
        }

        Ok(Self {
            string_table,
            column_metas,
            shared_column_metas,
            virtual_column_nodes,
        })
    }
}

fn get_arrow_schema_from_metadata(encoded_meta: &str) -> Result<Schema> {
    let decoded = BASE64_STANDARD.decode(encoded_meta);
    match decoded {
        Ok(bytes) => {
            let slice = if bytes.len() > 8 && bytes[0..4] == [255u8; 4] {
                &bytes[8..]
            } else {
                bytes.as_slice()
            };
            match arrow_ipc::root_as_message(slice) {
                Ok(message) => message
                    .header_as_schema()
                    .map(arrow_ipc::convert::fb_to_schema)
                    .ok_or_else(|| ErrorCode::Internal("the message is not Arrow Schema")),
                Err(err) => {
                    // The flatbuffers implementation returns an error on verification error.
                    Err(ErrorCode::Internal(format!(
                        "Unable to get root as message stored in virtual column ARROW:schema: {:?}",
                        err
                    )))
                }
            }
        }
        Err(err) => Err(ErrorCode::Internal(format!(
            "Unable to decode the encoded schema stored in virtual column ARROW:schema: {:?}",
            err
        ))),
    }
}

fn get_virtual_column_string_table(encoded_meta: &str) -> Result<Vec<String>> {
    serde_json::from_str(encoded_meta).map_err(|e| {
        ErrorCode::StorageOther(format!(
            "failed to decode virtual column string table {:?}",
            e
        ))
    })
}

fn get_virtual_column_nodes(encoded_meta: &str) -> Result<HashMap<u32, VirtualColumnNode>> {
    serde_json::from_str(encoded_meta).map_err(|e| {
        ErrorCode::StorageOther(format!("failed to decode virtual column nodes {:?}", e))
    })
}

fn get_virtual_column_shared_ids(encoded_meta: &str) -> Result<HashMap<u32, (u32, u32)>> {
    serde_json::from_str(encoded_meta).map_err(|e| {
        ErrorCode::StorageOther(format!(
            "failed to decode virtual column shared ids {:?}",
            e
        ))
    })
}

fn data_type_from_path(schema: &DataSchema, path: &[String]) -> Result<DataType> {
    if path.is_empty() {
        return Err(ErrorCode::Internal(
            "empty path in parquet schema".to_string(),
        ));
    }

    let field = schema.field_with_name(&path[0])?;
    let data_type = field.data_type().clone();
    if path.len() == 1 {
        return Ok(data_type);
    }

    if let DataType::Map(inner) = data_type.remove_nullable() {
        if let DataType::Tuple(inner_types) = inner.as_ref() {
            if inner_types.len() == 2 {
                let last = path.last().map(String::as_str).unwrap_or_default();
                if last == "key" || last == "value" {
                    return Ok(inner_types[1].clone());
                }
            }
        }
    }

    Err(ErrorCode::Internal(format!(
        "failed to find data type for path {:?}",
        path
    )))
}
