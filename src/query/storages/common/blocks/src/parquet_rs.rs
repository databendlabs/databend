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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::table::TableCompression;
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterVersion;
use parquet::format::FileMetaData;

/// Serialize data blocks to parquet format.
pub fn blocks_to_parquet(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    compression: TableCompression,
    enable_encoding: bool,
) -> Result<FileMetaData> {
    assert!(!blocks.is_empty());
    let builder = WriterProperties::builder()
        .set_compression(compression.into())
        // use `usize::MAX` to effectively limit the number of row groups to 1
        .set_max_row_group_size(usize::MAX)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false);

    let builder = if enable_encoding {
        // Enable dictionary encoding and fallback encodings.
        //
        // Memo for quick lookup:
        // The fallback encoding "strategy" used by parquet-54.2.1 is:
        //
        // ~~~
        //   (Type::BOOLEAN, WriterVersion::PARQUET_2_0) => Encoding::RLE,
        //   (Type::INT32, WriterVersion::PARQUET_2_0) => Encoding::DELTA_BINARY_PACKED,
        //   (Type::INT64, WriterVersion::PARQUET_2_0) => Encoding::DELTA_BINARY_PACKED,
        //   (Type::BYTE_ARRAY, WriterVersion::PARQUET_2_0) => Encoding::DELTA_BYTE_ARRAY,
        //   (Type::FIXED_LEN_BYTE_ARRAY, WriterVersion::PARQUET_2_0) => Encoding::DELTA_BYTE_ARRAY,
        //   _ => Encoding::PLAIN,
        // ~~~
        //
        builder
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(true)
    } else {
        builder
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
    };

    let props = builder.build();
    let batches = blocks
        .into_iter()
        .map(|block| block.to_record_batch(table_schema))
        .collect::<Result<Vec<_>>>()?;
    let arrow_schema = Arc::new(table_schema.into());
    let mut writer = ArrowWriter::try_new(write_buffer, arrow_schema, Some(props))?;
    for batch in batches {
        writer.write(&batch)?;
    }
    let file_meta = writer.close()?;
    Ok(file_meta)
}
