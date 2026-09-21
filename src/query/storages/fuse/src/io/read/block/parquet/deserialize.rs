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

use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use databend_common_expression::ColumnId;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::Compression;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::basic::Compression as ParquetCompression;

use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::io::read::block::parquet::adapter::RowGroupImplBuilder;

/// The returned record batch contains all deserialized columns in the same nested structure as the original schema.
pub fn column_chunks_to_record_batch(
    original_schema: &TableSchema,
    num_rows: usize,
    column_chunks: &HashMap<ColumnId, DataItem>,
    compression: &Compression,
    selection: Option<RowSelection>,
) -> databend_common_exception::Result<RecordBatch> {
    let arrow_schema = Schema::from(original_schema);
    let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema)?;

    let column_id_to_dfs_id = original_schema
        .to_leaf_column_ids()
        .iter()
        .enumerate()
        .map(|(dfs_id, column_id)| (*column_id, dfs_id))
        .collect::<HashMap<_, _>>();
    let mut projection_mask = Vec::with_capacity(column_chunks.len());
    let mut builder = RowGroupImplBuilder::new(
        num_rows,
        &parquet_schema,
        ParquetCompression::from(*compression),
    );
    for (column_id, data_item) in column_chunks.iter() {
        match data_item {
            DataItem::RawData(bytes) => {
                let dfs_id = column_id_to_dfs_id.get(column_id).cloned().unwrap();
                projection_mask.push(dfs_id);
                builder.add_column_chunk(dfs_id, bytes.clone());
            }
            DataItem::ColumnArray(_) => {}
        }
    }
    projection_mask.sort_unstable();
    let row_group = Box::new(builder.build());
    let field_levels = parquet_to_arrow_field_levels(
        &parquet_schema,
        ProjectionMask::leaves(&parquet_schema, projection_mask),
        Some(arrow_schema.fields()),
    )?;
    // The reader uses batch_size to preallocate column buffers retained by the output.
    let batch_size = selection.as_ref().map_or(num_rows, RowSelection::row_count);
    let mut record_reader = ParquetRecordBatchReader::try_new_with_row_groups(
        &field_levels,
        row_group.as_ref(),
        batch_size,
        selection,
    )?;
    let record = match record_reader.next() {
        Some(record) => record?,
        // The reader yields nothing when `selection` keeps no rows, e.g. a prewhere
        // predicate filtered out every row of the block while the block itself
        // survived min/max pruning. Return an empty batch with the projected schema
        // so callers can build zero-row columns without special casing.
        None => RecordBatch::new_empty(record_reader.schema()),
    };
    assert!(record_reader.next().is_none());
    Ok(record)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use bytes::Bytes;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::NumberDataType;
    use opendal::Buffer;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;

    #[test]
    fn test_selection_buffer_capacity() -> databend_common_exception::Result<()> {
        let num_rows = 4096;
        let schema = TableSchema::new(vec![TableField::new(
            "n",
            TableDataType::Number(NumberDataType::Int64),
        )]);
        let arrow_schema = Arc::new(Schema::from(&schema));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int64Array::from_iter_values(0..num_rows as i64),
        )])?;
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_row_count_limit(128)
            .set_write_batch_size(128)
            .build();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, arrow_schema, Some(properties))?;
        writer.write(&batch)?;
        let metadata = writer.close()?;
        let (offset, length) = metadata.row_group(0).column(0).byte_range();
        let bytes = Bytes::from(bytes);
        let column_chunks = HashMap::from([(
            schema.field(0).column_id(),
            DataItem::RawData(Buffer::from(
                bytes.slice(offset as usize..(offset + length) as usize),
            )),
        )]);

        for selected in [
            Some(vec![1, 2, num_rows - 1]),
            Some((0..num_rows).step_by(2).collect()),
            Some(vec![]),
            None,
        ] {
            let selection = selected.as_ref().map(|rows| {
                RowSelection::from_consecutive_ranges(
                    rows.iter().map(|&row| row..row + 1),
                    num_rows,
                )
            });
            let expected: Vec<i64> = selected
                .unwrap_or_else(|| (0..num_rows).collect())
                .into_iter()
                .map(|row| row as i64)
                .collect();
            let record = column_chunks_to_record_batch(
                &schema,
                num_rows,
                &column_chunks,
                &Compression::None,
                selection,
            )?;
            let values = record
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values.values().as_ref(), expected.as_slice());
            // Include spare capacity when checking the retained allocation.
            let capacity = values.values().inner().capacity();
            assert!(
                capacity <= expected.len().max(8) * size_of::<i64>(),
                "{} selected rows retained {capacity} bytes",
                expected.len(),
            );
        }
        Ok(())
    }
}
