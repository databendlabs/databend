// Copyright Qdrant
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

mod common;
mod entry_points;
mod graph_layers;
mod graph_layers_builder;
mod graph_links;
mod hnsw;
mod point_scorer;
mod quantization;
mod search_context;
mod visited_pool;

use std::collections::BTreeMap;

use bytes::Bytes;
pub use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
pub use common::types::ScoredPointOffset;
use databend_common_exception::ErrorCode;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
pub use hnsw::HNSWIndex;
use parquet::format::FileMetaData;
pub use quantization::DistanceType;

#[derive(Clone)]
pub struct VectorIndexMeta {
    pub columns: Vec<(String, SingleColumnMeta)>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct VectorIndexFile {
    pub name: String,
    pub data: Bytes,
}

impl VectorIndexFile {
    pub fn create(name: String, data: Bytes) -> Self {
        Self { name, data }
    }
}

impl TryFrom<FileMetaData> for VectorIndexMeta {
    type Error = ErrorCode;

    fn try_from(mut meta: FileMetaData) -> std::result::Result<Self, Self::Error> {
        let rg = meta.row_groups.remove(0);
        let mut col_metas = Vec::with_capacity(rg.columns.len());
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
                    let column_name = chunk_meta.path_in_schema[0].to_owned();
                    col_metas.push((column_name, res));
                }
                None => {
                    panic!(
                        "expecting chunk meta data while converting ThriftFileMetaData to BloomIndexMeta"
                    )
                }
            }
        }
        col_metas.shrink_to_fit();

        let mut metadata = BTreeMap::new();
        if let Some(key_value_metadata) = meta.key_value_metadata {
            for key_value in &key_value_metadata {
                if key_value.key == "ARROW:schema" || key_value.value.is_none() {
                    continue;
                }
                metadata.insert(key_value.key.clone(), key_value.value.clone().unwrap());
            }
        }

        Ok(Self {
            columns: col_metas,
            metadata,
        })
    }
}
