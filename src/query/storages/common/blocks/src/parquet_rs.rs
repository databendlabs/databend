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

use std::sync::Arc;

use arrow_schema::Schema;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::table::TableCompression;
use parquet_rs::arrow::ArrowWriter;
use parquet_rs::basic::Compression;
use parquet_rs::basic::Encoding;
use parquet_rs::basic::ZstdLevel;
use parquet_rs::file::properties::EnabledStatistics;
use parquet_rs::file::properties::WriterProperties;
use parquet_rs::format::FileMetaData;

/// Serialize data blocks to parquet format.
pub fn blocks_to_parquet(
    schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    compression: TableCompression,
) -> Result<FileMetaData> {
    let compression = to_parquet_rs_compression(compression);
    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_data_page_size_limit(usize::MAX)
        .set_max_row_group_size(usize::MAX)
        .set_encoding(Encoding::PLAIN)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .build();
    let arrow_schema = Arc::new(Schema::from(schema));
    let mut writer = ArrowWriter::try_new(write_buffer, arrow_schema.clone(), Some(props))?;
    for block in blocks {
        let batch = block.to_record_batch_with_arrow_schema(arrow_schema.clone())?;
        writer.write(&batch)?;
    }
    let file_meta = writer.close()?;
    Ok(file_meta)
}

fn to_parquet_rs_compression(compression: TableCompression) -> Compression {
    match compression {
        TableCompression::None => Compression::UNCOMPRESSED,
        TableCompression::LZ4 => Compression::LZ4_RAW,
        TableCompression::Snappy => Compression::SNAPPY,
        TableCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
    }
}
