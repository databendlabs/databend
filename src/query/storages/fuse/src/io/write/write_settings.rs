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

use databend_common_expression::ColumnId;
use databend_common_io::constants::DEFAULT_BLOCK_PER_SEGMENT;
use databend_storages_common_index::BloomIndexType;
use databend_storages_common_table_meta::table::TableCompression;

use crate::FuseStorageFormat;

pub const MAX_BLOCK_UNCOMPRESSED_SIZE: usize = 1024 * 1024 * 400;

#[derive(Clone, Debug)]
pub struct WriteSettings {
    pub storage_format: FuseStorageFormat,
    pub table_compression: TableCompression,
    pub bloom_index_type: BloomIndexType,

    pub block_per_seg: usize,
    pub enable_parquet_dictionary: bool,
    pub data_page_rows: Option<usize>,
    pub data_page_bytes: Option<usize>,
    pub col_stats_truncate_lens: BTreeMap<ColumnId, usize>,
}

impl Default for WriteSettings {
    fn default() -> Self {
        Self {
            storage_format: FuseStorageFormat::Parquet,
            table_compression: TableCompression::default(),
            bloom_index_type: BloomIndexType::default(),
            block_per_seg: DEFAULT_BLOCK_PER_SEGMENT,
            enable_parquet_dictionary: false,
            data_page_rows: None,
            data_page_bytes: None,
            col_stats_truncate_lens: BTreeMap::new(),
        }
    }
}
