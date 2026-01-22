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

use arrow_array::RecordBatch;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parquet::arrow::ArrowWriter;

/// Parquet page size hard limit (~2GB). Arrow-rs 56.2.0+ errors if exceeded.
const PARQUET_PAGE_SIZE_HARD_LIMIT: usize = i32::MAX as usize - (1 << 20);

/// Soft limit for batch splitting. We use 64MB (vs 2GB hard limit) to provide safety
/// margin for estimation inaccuracy when row sizes are not uniform.
pub const MAX_BATCH_MEMORY_SIZE: usize = 1 << 26; // 64MB

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

    // Fast path: batch fits within limit, no split needed.
    let batch_size = batch.get_array_memory_size();
    if batch_size <= max_batch_size {
        writer.write(batch)?;
        return Ok(());
    }

    // Single row: allow if under Parquet's hard limit (~2GB), even if above soft limit.
    if num_rows == 1 {
        if batch_size > PARQUET_PAGE_SIZE_HARD_LIMIT {
            return Err(ErrorCode::Internal(format!(
                "A single row requires {} bytes which exceeds Parquet's page size limit ({} bytes).",
                batch_size, PARQUET_PAGE_SIZE_HARD_LIMIT
            )));
        }
        // Single row above soft limit but under hard limit: write directly.
        writer.write(batch)?;
        return Ok(());
    }

    // Calculate rows per chunk assuming uniform row size.
    // Note: RecordBatch::slice() shares underlying buffers, so we estimate by ratio.
    let rows_per_chunk =
        ((num_rows as f64 * max_batch_size as f64) / batch_size as f64).ceil() as usize;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::StringType;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    use super::write_batch_with_page_limit;

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
        let schema = TableSchema::new(vec![TableField::new("big_string", TableDataType::String)]);

        // Create 100 rows with 1MB strings each = 100MB total.
        // Use a small page limit (10MB) for testing.
        let test_page_limit = 10 * 1024 * 1024; // 10MB
        let big_string = "x".repeat(1024 * 1024); // 1MB per value
        let values: Vec<String> = (0..100).map(|_| big_string.clone()).collect();

        let string_column = StringType::from_data(values.clone());
        let block = DataBlock::new_from_columns(vec![string_column]);

        let arrow_schema = Arc::new((&schema).into());
        let batch = block
            .to_record_batch_with_arrow_schema(&arrow_schema)
            .unwrap();

        let batch_size = batch.get_array_memory_size();
        assert!(
            batch_size > test_page_limit,
            "Test setup error: batch size {} should exceed limit {}",
            batch_size,
            test_page_limit
        );

        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema, Some(props)).unwrap();

        write_batch_with_page_limit(&mut writer, &batch, test_page_limit).unwrap();
        let _ = writer.close().unwrap();

        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        let row_group = reader.get_row_group(0).unwrap();
        let mut page_reader = row_group.get_column_page_reader(0).unwrap();

        let mut page_count = 0;
        let mut total_values = 0;
        while let Some(page) = page_reader.get_next_page().unwrap() {
            page_count += 1;
            total_values += page.num_values() as usize;
        }

        assert!(
            page_count > 1,
            "Expected multiple pages due to splitting, but got {} page(s). \
             Batch size: {}, Page limit: {}",
            page_count,
            batch_size,
            test_page_limit
        );

        assert_eq!(
            total_values, 100,
            "Expected 100 values total across all pages, got {}",
            total_values
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

        write_batch_with_page_limit(&mut writer, &batch, 512 * 1024)
            .expect("single large row under hard limit should succeed");
        writer.close().unwrap();

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

        // Create 20 rows with 1MB strings each = 20MB total.
        let big_string = "x".repeat(1024 * 1024);
        let values: Vec<String> = (0..20).map(|_| big_string.clone()).collect();

        let string_column = StringType::from_data(values.clone());
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

        writer.write(&batch).unwrap();
        let _ = writer.close().unwrap();

        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        let row_group = reader.get_row_group(0).unwrap();
        let mut page_reader = row_group.get_column_page_reader(0).unwrap();

        let mut page_count = 0;
        while let Some(_page) = page_reader.get_next_page().unwrap() {
            page_count += 1;
        }

        assert_eq!(
            page_count, 1,
            "Without splitting, expected exactly 1 page, got {}",
            page_count
        );
    }
}
