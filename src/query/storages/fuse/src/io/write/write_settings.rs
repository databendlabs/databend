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

use databend_common_io::constants::DEFAULT_BLOCK_PER_SEGMENT;
use databend_storages_common_blocks::ParquetWriteOptions;
use databend_storages_common_table_meta::table::TableCompression;

use crate::DEFAULT_ROW_PER_PAGE;
use crate::FuseStorageFormat;

pub const MAX_BLOCK_UNCOMPRESSED_SIZE: usize = 1024 * 1024 * 400;

#[derive(Clone, Debug)]
pub struct WriteSettings {
    pub storage_format: FuseStorageFormat,
    pub table_compression: TableCompression,
    // rows per page, current only work in native format
    pub max_page_size: usize,

    pub block_per_seg: usize,
    pub enable_parquet_dictionary: bool,
    pub enable_parquet_delta_binary_packed_heuristic_rule: bool,
}

impl Default for WriteSettings {
    fn default() -> Self {
        Self {
            storage_format: FuseStorageFormat::Parquet,
            table_compression: TableCompression::default(),
            max_page_size: DEFAULT_ROW_PER_PAGE,
            block_per_seg: DEFAULT_BLOCK_PER_SEGMENT,
            enable_parquet_dictionary: false,
            enable_parquet_delta_binary_packed_heuristic_rule: true,
        }
    }
}

impl WriteSettings {
    pub fn parquet_options(&self) -> ParquetWriteOptions {
        ParquetWriteOptions::builder(self.table_compression)
            .enable_dictionary(self.enable_parquet_dictionary)
            .enable_delta_binary_packed_heuristic_rule(
                self.enable_parquet_delta_binary_packed_heuristic_rule,
            )
            .build()
    }
}
