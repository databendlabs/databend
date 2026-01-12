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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct VirtualColumnFileMeta {
    pub columns: HashMap<String, SingleColumnMeta>,
    pub data_types: HashMap<String, DataType>,
    pub shared_names: BTreeMap<u32, Vec<String>>,
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

impl TryFrom<FileMetaData> for VirtualColumnFileMeta {
    type Error = ErrorCode;

    fn try_from(mut meta: FileMetaData) -> std::result::Result<Self, Self::Error> {
        let rg = meta.row_groups.remove(0);
        let mut col_metas = HashMap::with_capacity(rg.columns.len());
        for x in &rg.columns {
            match &x.meta_data {
                Some(chunk_meta) => {
                    let col_start =
                        if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
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
                    let column_name = chunk_meta.path_in_schema.join(".");
                    col_metas.insert(column_name, res);
                }
                None => {
                    panic!(
                        "expecting chunk meta data while converting ThriftFileMetaData to VirtualColumnFileMeta"
                    )
                }
            }
        }

        let mut data_types = HashMap::new();
        let mut shared_names = BTreeMap::new();
        let key_value_metadata = meta.key_value_metadata.unwrap_or_default();
        for key_value in &key_value_metadata {
            if key_value.key == "ARROW:schema" {
                let encoded_meta = key_value.value.as_ref().unwrap();
                let schema = get_arrow_schema_from_metadata(encoded_meta).unwrap();
                let data_schema = DataSchema::try_from(&schema).unwrap();
                for field in data_schema.fields() {
                    data_types.insert(field.name().clone(), field.data_type().clone());
                }
            } else if key_value.key == "shared_virtual_column_key_names" {
                let encoded_meta = key_value.value.as_ref().unwrap();
                shared_names = get_shared_virtual_column_names(encoded_meta).unwrap();
            }
        }

        Ok(Self {
            columns: col_metas,
            data_types,
            shared_names,
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

fn get_shared_virtual_column_names(
    encoded_shared_names: &str,
) -> Result<BTreeMap<u32, Vec<String>>> {
    let mut shared_virtual_column_names = BTreeMap::new();
    let v: serde_json::Value = serde_json::from_str(encoded_shared_names)?;
    if let Some(obj) = v.as_object() {
        for (key_str, val) in obj.iter() {
            let Ok(source_column_id) = key_str.parse::<u32>() else {
                continue;
            };
            let Some(vals) = val.as_array() else {
                continue;
            };
            let names: Vec<String> = vals
                .iter()
                .map(|val| val.as_str().unwrap().to_string())
                .collect();
            shared_virtual_column_names.insert(source_column_id, names);
        }
    }
    Ok(shared_virtual_column_names)
}
