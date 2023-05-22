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

use std::io::Bytes;

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::io::parquet::write::transverse;
use common_arrow::arrow::io::parquet::write::RowGroupIterator;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::arrow_array::RecordBatch;
use common_arrow::parquet::arrow::arrow_writer::ArrowWriter;
use common_arrow::parquet::file::properties::WriterProperties;
use common_arrow::parquet::format::FileMetaData;
use common_arrow::parquet2::encoding::Encoding;
use common_arrow::parquet2::metadata::ThriftFileMetaData;
use common_arrow::parquet2::write::Version;
use common_arrow::write_parquet_file;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::TableSchema;
use storages_common_table_meta::table::TableCompression;

/// Serialize data blocks to parquet format.
pub fn blocks_to_parquet(
    schema: impl AsRef<TableSchema>,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    compression: TableCompression,
) -> Result<(u64, FileMetaData)> {
    let start_pos = write_buffer.len() as u64;
    let data_schema: DataSchema = schema.into();
    let batches: Vec<RecordBatch> = blocks
        .into_iter()
        .map(|block| block.to_record_batch(&data_schema).unwrap())
        .collect();
    assert!(batches.len() > 0);
    let props = WriterProperties::builder()
        .set_compression(compression.into())
        .build();
    let mut writer =
        ArrowWriter::try_new(&mut *write_buffer, batches[0].schema(), Some(props)).unwrap();

    batches.iter().try_for_each(|batch| writer.write(batch))?;

    match writer.close() {
        Ok(meta) => Ok((write_buffer.len() as u64 - start_pos, meta)),
        Err(cause) => Err(ErrorCode::Internal(format!(
            "write_parquet_file: {:?}",
            cause,
        ))),
    }
}
