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

use std::io::Write;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::converts::arrow::table_schema_to_arrow_schema;
use databend_common_expression::infer_table_schema;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::table::TableCompression;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::ChunkReader;
use parquet::format::FileMetaData;

/// Serialize data blocks to parquet format.
pub fn blocks_to_parquet<W: Write + Send>(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: W,
    compression: TableCompression,
) -> Result<FileMetaData> {
    assert!(!blocks.is_empty());
    let props = WriterProperties::builder()
        .set_compression(compression.into())
        // use `usize::MAX` to effectively limit the number of row groups to 1
        .set_max_row_group_size(usize::MAX)
        .set_encoding(Encoding::PLAIN)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .build();
    let batches = blocks
        .into_iter()
        .map(|block| block.to_record_batch(table_schema))
        .collect::<Result<Vec<_>>>()?;
    let arrow_schema = Arc::new(table_schema_to_arrow_schema(table_schema));
    let mut writer = ArrowWriter::try_new(write_buffer, arrow_schema, Some(props))?;
    for batch in batches {
        writer.write(&batch)?;
    }
    let file_meta = writer.close()?;
    Ok(file_meta)
}

/// Serialize bare data blocks to parquet format.
pub fn bare_blocks_to_parquet<W: Write + Send>(
    blocks: Vec<DataBlock>,
    write_buffer: W,
    compression: TableCompression,
) -> Result<FileMetaData> {
    let data_schema = fake_data_schema(blocks.first().unwrap());
    let table_schema = infer_table_schema(&data_schema)?;

    blocks_to_parquet(&table_schema, blocks, write_buffer, compression)
}

fn fake_data_schema(block: &DataBlock) -> DataSchema {
    let fields = block
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, arg)| DataField::new(&format!("arg{}", idx + 1), arg.data_type.clone()))
        .collect::<Vec<_>>();
    DataSchema::new(fields)
}

/// Deserialize bare data block from parquet format.
pub fn bare_blocks_from_parquet<R: ChunkReader + 'static>(data: R) -> Result<DataBlock> {
    let reader = ParquetRecordBatchReader::try_new(data, usize::MAX)?;
    let mut blocks = Vec::with_capacity(1);
    for record_batch in reader {
        let record_batch = record_batch?;
        let schema = DataSchema::try_from(record_batch.schema().as_ref())?;
        let (block, _) = DataBlock::from_record_batch(&schema, &record_batch)?;
        blocks.push(block);
    }
    DataBlock::concat(&blocks)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use databend_common_expression::block_debug::assert_block_value_eq;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::StringType;
    use databend_common_expression::FromData;

    use super::*;

    #[test]
    fn test_serde_bin_column() -> Result<()> {
        let blocks = vec![
            [
                StringType::from_data(vec!["SM CASE", "a"]),
                StringType::from_data(vec!["SM CASE", "axx"]),
                Int64Type::from_data(vec![1, 3]),
            ],
            [
                StringType::from_data(vec!["b", "e", "f", "g"]),
                StringType::from_data(vec!["", "", "", "x"]),
                Int64Type::from_data(vec![99, 7, 3, 4]),
            ],
        ]
        .into_iter()
        .map(|columns| DataBlock::new_from_columns(columns.to_vec()))
        .collect::<Vec<_>>();

        let mut data = Vec::new();
        bare_blocks_to_parquet(blocks.clone(), &mut data, TableCompression::LZ4)?;

        let got = bare_blocks_from_parquet(Bytes::from(data))?;
        let want = DataBlock::concat(&blocks)?;

        assert_block_value_eq(&want, &got);

        Ok(())
    }
}
