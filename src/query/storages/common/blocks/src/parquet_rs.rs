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

use arrow_array::RecordBatch;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_common_expression::converts::arrow::table_schema_arrow_leaf_paths;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::table::TableCompression;
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterVersion;
use parquet::format::FileMetaData;
use parquet::schema::types::ColumnPath;

/// Disable dictionary encoding once the NDV-to-row ratio is greater than this threshold.
const HIGH_CARDINALITY_RATIO_THRESHOLD: f64 = 0.1;

/// Parquet page size hard limit (~2GB). Arrow-rs 56.2.0+ errors if exceeded.
const PARQUET_PAGE_SIZE_HARD_LIMIT: usize = i32::MAX as usize - (1 << 20);

/// Soft limit for batch splitting. We use 64MB (vs 2GB hard limit) to provide safety
/// margin for estimation inaccuracy when row sizes are not uniform.
pub const MAX_BATCH_MEMORY_SIZE: usize = 1 << 26; // 64MB

/// Serialize data blocks to parquet format.
pub fn blocks_to_parquet(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    compression: TableCompression,
    enable_dictionary: bool,
    metadata: Option<Vec<KeyValue>>,
) -> Result<FileMetaData> {
    blocks_to_parquet_with_stats(
        table_schema,
        blocks,
        write_buffer,
        compression,
        enable_dictionary,
        metadata,
        None,
    )
}

/// Serialize blocks while optionally tuning dictionary behavior via NDV statistics.
///
/// * `table_schema` - Logical schema used to build Arrow batches.
/// * `blocks` - In-memory blocks that will be serialized into a single Parquet file.
/// * `write_buffer` - Destination buffer that receives the serialized Parquet bytes.
/// * `compression` - Compression algorithm specified by table-level settings.
/// * `enable_dictionary` - Enables dictionary encoding globally before per-column overrides.
/// * `metadata` - Additional user metadata embedded into the Parquet footer.
/// * `column_stats` - Optional NDV stats from the first block, used to configure writer properties
///   before ArrowWriter instantiation disables further changes.
pub fn blocks_to_parquet_with_stats(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    compression: TableCompression,
    enable_dictionary: bool,
    metadata: Option<Vec<KeyValue>>,
    column_stats: Option<&StatisticsOfColumns>,
) -> Result<FileMetaData> {
    assert!(!blocks.is_empty());

    // Writer properties cannot be tweaked after ArrowWriter creation, so we mirror the behavior of
    // the streaming writer and only rely on the first block's NDV (and row count) snapshot.
    let num_rows = blocks[0].num_rows();
    let arrow_schema = Arc::new(table_schema.into());

    let props = build_parquet_writer_properties(
        compression,
        enable_dictionary,
        column_stats,
        metadata,
        num_rows,
        table_schema,
    );

    let batches = blocks
        .into_iter()
        .map(|block| block.to_record_batch_with_arrow_schema(&arrow_schema))
        .collect::<Result<Vec<_>>>()?;

    let mut writer = ArrowWriter::try_new(write_buffer, arrow_schema, Some(props))?;
    for batch in batches {
        write_batch_with_page_limit(&mut writer, &batch, MAX_BATCH_MEMORY_SIZE)?;
    }
    let file_meta = writer.close()?;
    Ok(file_meta)
}

/// Split large batches before writing to avoid arrow-rs page size errors.
///
/// Calculates rows per chunk assuming uniform row size (RecordBatch::slice shares buffers,
/// so sliced sizes cannot be measured). Single rows exceeding soft limit but under hard
/// limit (~2GB) are allowed.
///
/// LIMITATION: Assumes uniform row sizes. A complete solution requires enforcing max size
/// limits on variable-length types (String, Variant, etc.) at the ingestion layer.
pub fn write_batch_with_page_limit<W: std::io::Write + Send>(
    writer: &mut ArrowWriter<W>,
    batch: &RecordBatch,
    max_batch_size: usize,
) -> Result<()> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        writer.write(batch)?;
        return Ok(());
    }

    // Fast path: batch fits within limit, no split needed
    let batch_size = batch.get_array_memory_size();
    if batch_size <= max_batch_size {
        writer.write(batch)?;
        return Ok(());
    }

    // Single row: allow if under Parquet's hard limit (~2GB), even if above soft limit
    if num_rows == 1 {
        if batch_size > PARQUET_PAGE_SIZE_HARD_LIMIT {
            return Err(ErrorCode::Internal(format!(
                "A single row requires {} bytes which exceeds Parquet's page size limit ({} bytes).",
                batch_size, PARQUET_PAGE_SIZE_HARD_LIMIT
            )));
        }
        // Single row above soft limit but under hard limit: write directly
        writer.write(batch)?;
        return Ok(());
    }

    // Calculates rows per chunk upfront (assuming uniform row size).
    // Note: RecordBatch::slice() shares underlying buffers, so get_array_memory_size()
    // on sliced batches returns the full buffer size. We estimate chunk size by ratio.
    let target_size = max_batch_size * 9 / 10; // 90% of limit for safety margin
    let rows_per_chunk =
        ((num_rows as f64 * target_size as f64) / batch_size as f64).ceil() as usize;
    let rows_per_chunk = rows_per_chunk.max(1);

    let mut offset = 0;
    while offset < num_rows {
        let length = rows_per_chunk.min(num_rows - offset);
        let chunk = batch.slice(offset, length);
        writer.write(&chunk)?;
        offset += length;
    }

    Ok(())
}

/// Create writer properties, optionally disabling dictionaries for high-cardinality columns.
pub fn build_parquet_writer_properties(
    compression: TableCompression,
    enable_dictionary: bool,
    cols_stats: Option<impl NdvProvider>,
    metadata: Option<Vec<KeyValue>>,
    num_rows: usize,
    table_schema: &TableSchema,
) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_compression(compression.into())
        // use `usize::MAX` to effectively limit the number of row groups to 1
        .set_max_row_group_size(usize::MAX)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_key_value_metadata(metadata);

    if enable_dictionary {
        // Enable dictionary for all columns
        builder = builder
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(true);
        if let Some(cols_stats) = cols_stats {
            // Disable dictionary of columns that have high cardinality
            for (column_id, components) in table_schema_arrow_leaf_paths(table_schema) {
                if let Some(ndv) = cols_stats.column_ndv(&column_id) {
                    if num_rows > 0
                        && (ndv as f64 / num_rows as f64) > HIGH_CARDINALITY_RATIO_THRESHOLD
                    {
                        builder = builder
                            .set_column_dictionary_enabled(ColumnPath::from(components), false);
                    }
                }
            }
        }
        builder.build()
    } else {
        builder.set_dictionary_enabled(false).build()
    }
}

/// Provides per column NDV statistics
pub trait NdvProvider {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64>;
}

impl NdvProvider for &StatisticsOfColumns {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
        self.get(column_id).and_then(|item| item.distinct_of_values)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::number::NumberDataType;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    use super::*;

    struct TestNdvProvider {
        ndv: HashMap<ColumnId, u64>,
    }

    impl NdvProvider for TestNdvProvider {
        fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
            self.ndv.get(column_id).copied()
        }
    }

    fn sample_schema() -> TableSchema {
        TableSchema::new(vec![
            TableField::new("simple", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("nested", TableDataType::Tuple {
                fields_name: vec!["leaf".to_string(), "arr".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int64),
                    TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::UInt64))),
                ],
            }),
            TableField::new("no_stats", TableDataType::String),
        ])
    }

    fn column_id(schema: &TableSchema, name: &str) -> ColumnId {
        schema
            .leaf_fields()
            .into_iter()
            .find(|field| field.name() == name)
            .unwrap_or_else(|| panic!("missing field {}", name))
            .column_id()
    }

    #[test]
    fn test_build_parquet_writer_properties_handles_nested_leaves() {
        let schema = sample_schema();

        let mut ndv = HashMap::new();
        ndv.insert(column_id(&schema, "simple"), 500);
        ndv.insert(column_id(&schema, "nested:leaf"), 50);
        ndv.insert(column_id(&schema, "nested:arr:0"), 400);

        let column_paths: HashMap<ColumnId, ColumnPath> = table_schema_arrow_leaf_paths(&schema)
            .into_iter()
            .map(|(id, path)| (id, ColumnPath::from(path)))
            .collect();

        let props = build_parquet_writer_properties(
            TableCompression::Zstd,
            true,
            Some(TestNdvProvider { ndv }),
            None,
            1000,
            &schema,
        );

        assert!(
            !props.dictionary_enabled(&column_paths[&column_id(&schema, "simple")]),
            "high cardinality top-level column should disable dictionary"
        );
        assert!(
            props.dictionary_enabled(&column_paths[&column_id(&schema, "nested:leaf")]),
            "low cardinality nested column should keep dictionary"
        );
        assert!(
            !props.dictionary_enabled(&column_paths[&column_id(&schema, "nested:arr:0")]),
            "high cardinality nested array element should disable dictionary"
        );
        assert!(
            props.dictionary_enabled(&column_paths[&column_id(&schema, "no_stats")]),
            "columns without NDV stats keep the default dictionary behavior"
        );
    }

    #[test]
    fn test_build_parquet_writer_properties_disabled_globally() {
        let schema = sample_schema();

        let column_paths: HashMap<ColumnId, ColumnPath> = table_schema_arrow_leaf_paths(&schema)
            .into_iter()
            .map(|(id, path)| (id, ColumnPath::from(path)))
            .collect();

        let props = build_parquet_writer_properties(
            TableCompression::Zstd,
            false,
            None::<TestNdvProvider>,
            None,
            1000,
            &schema,
        );

        for field in schema.leaf_fields() {
            assert!(
                !props.dictionary_enabled(&column_paths[&field.column_id()]),
                "dictionary must remain disabled when enable_dictionary is false",
            );
        }
    }

    /// Test that large batches are split to prevent page size overflow.
    ///
    /// ArrowWriter splits data into mini-batches of `write_batch_size` (default 1024 rows),
    /// and only checks page size limits after each mini-batch. If a mini-batch exceeds 2GB,
    /// it still becomes a single page, causing i32 overflow in `uncompressed_page_size`.
    ///
    /// This test verifies that our `write_batch_with_page_limit` function correctly splits
    /// large batches before passing to ArrowWriter, preventing this overflow.
    #[test]
    fn test_large_batch_is_split_to_prevent_page_overflow() {
        // Create a schema with a string column
        let schema = TableSchema::new(vec![TableField::new("big_string", TableDataType::String)]);

        // Create 100 rows with 1MB strings each = 100MB total
        // Use a small page limit (10MB) for testing
        let test_page_limit = 10 * 1024 * 1024; // 10MB
        let big_string = "x".repeat(1024 * 1024); // 1MB per value
        let values: Vec<String> = (0..100).map(|_| big_string.clone()).collect();

        let string_column = StringType::from_data(values.clone());
        let block = DataBlock::new_from_columns(vec![string_column]);

        // Write using blocks_to_parquet (with our custom limit for testing)
        let arrow_schema = Arc::new((&schema).into());
        let batch = block
            .to_record_batch_with_arrow_schema(&arrow_schema)
            .unwrap();

        // Verify the batch size exceeds our test limit
        let batch_size = batch.get_array_memory_size();
        assert!(
            batch_size > test_page_limit,
            "Test setup error: batch size {} should exceed limit {}",
            batch_size,
            test_page_limit
        );

        // Write with page limit splitting
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema, Some(props)).unwrap();

        // Use our splitting function with the test limit
        write_batch_with_page_limit(&mut writer, &batch, test_page_limit).unwrap();
        let _ = writer.close().unwrap();

        // Read the parquet file and count pages
        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        let row_group = reader.get_row_group(0).unwrap();
        let mut page_reader = row_group.get_column_page_reader(0).unwrap();

        let mut page_count = 0;
        let mut total_values = 0;
        while let Some(page) = page_reader.get_next_page().unwrap() {
            page_count += 1;
            total_values += page.num_values() as usize;
        }

        // Verify that the batch was split into multiple pages
        assert!(
            page_count > 1,
            "Expected multiple pages due to splitting, but got {} page(s). \
             Batch size: {}, Page limit: {}",
            page_count,
            batch_size,
            test_page_limit
        );

        // Verify all values were written
        assert_eq!(
            total_values, 100,
            "Expected 100 values total across all pages, got {}",
            total_values
        );

        println!(
            "Test passed: {} pages created for {} values with {}MB batch and {}MB limit",
            page_count,
            total_values,
            batch_size / 1024 / 1024,
            test_page_limit / 1024 / 1024
        );
    }

    /// Test that a single large row (above soft limit but under hard limit) is allowed.
    #[test]
    fn test_single_large_row_under_hard_limit_succeeds() {
        let schema = TableSchema::new(vec![TableField::new("big_string", TableDataType::String)]);
        let large_value = "x".repeat(2 * 1024 * 1024); // 2MB - above soft limit, under hard limit
        let string_column = StringType::from_data(vec![large_value]);
        let block = DataBlock::new_from_columns(vec![string_column]);

        let arrow_schema = Arc::new((&schema).into());
        let batch = block
            .to_record_batch_with_arrow_schema(&arrow_schema)
            .unwrap();

        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema, Some(props)).unwrap();

        // Should succeed: single row above soft limit (512KB) but under hard limit (~2GB)
        write_batch_with_page_limit(&mut writer, &batch, 512 * 1024)
            .expect("single large row under hard limit should succeed");
        writer.close().unwrap();

        // Verify the data was written
        assert!(!buffer.is_empty(), "Parquet file should be written");
    }

    /// Test that without splitting, a large batch creates only one page.
    ///
    /// This demonstrates the problematic behavior: ArrowWriter's internal mini-batch size
    /// (default 1024 rows) determines page boundaries. When rows are small enough to fit
    /// within a mini-batch, all data ends up in a single page regardless of total size.
    /// Our `write_batch_with_page_limit` function addresses this by pre-splitting batches.
    #[test]
    fn test_without_splitting_creates_single_page() {
        let schema = TableSchema::new(vec![TableField::new("big_string", TableDataType::String)]);

        // Create 20 rows with 1MB strings each = 20MB total
        let big_string = "x".repeat(1024 * 1024);
        let values: Vec<String> = (0..20).map(|_| big_string.clone()).collect();

        let string_column = StringType::from_data(values.clone());
        let block = DataBlock::new_from_columns(vec![string_column]);

        let arrow_schema = Arc::new((&schema).into());
        let batch = block
            .to_record_batch_with_arrow_schema(&arrow_schema)
            .unwrap();

        // Write WITHOUT splitting (direct write)
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema, Some(props)).unwrap();

        // Direct write without splitting
        writer.write(&batch).unwrap();
        let _ = writer.close().unwrap();

        // Count pages
        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        let row_group = reader.get_row_group(0).unwrap();
        let mut page_reader = row_group.get_column_page_reader(0).unwrap();

        let mut page_count = 0;
        while let Some(_page) = page_reader.get_next_page().unwrap() {
            page_count += 1;
        }

        // Without splitting, all data goes into a single page
        // (This is the problematic behavior we're fixing)
        assert_eq!(
            page_count, 1,
            "Without splitting, expected exactly 1 page, got {}",
            page_count
        );
    }
}
