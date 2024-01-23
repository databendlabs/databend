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

#![allow(clippy::uninlined_format_args)]

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::table::TableCompression;

mod parquet2;
mod parquet_rs;

pub enum ParquetFileMeta {
    Parquet2(parquet_format_safe::FileMetaData),
    ParquetRs(::parquet_rs::format::FileMetaData),
}

pub fn blocks_to_parquet(
    schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    compression: TableCompression,
    use_parquet2: bool,
) -> Result<ParquetFileMeta> {
    match use_parquet2 {
        true => parquet2::blocks_to_parquet(schema, blocks, write_buffer, compression)
            .map(|(_, meta)| ParquetFileMeta::Parquet2(meta)),
        false => parquet_rs::blocks_to_parquet(schema, blocks, write_buffer, compression)
            .map(ParquetFileMeta::ParquetRs),
    }
}
