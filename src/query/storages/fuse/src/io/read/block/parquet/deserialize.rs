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
use parquet::schema::types::SchemaDescriptor;

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
    column_chunks_to_record_batch_with_schema(
        original_schema,
        &arrow_schema,
        &parquet_schema,
        num_rows,
        column_chunks,
        compression,
        selection,
    )
}

pub fn column_chunks_to_record_batch_with_schema(
    original_schema: &TableSchema,
    arrow_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
    num_rows: usize,
    column_chunks: &HashMap<ColumnId, DataItem>,
    compression: &Compression,
    selection: Option<RowSelection>,
) -> databend_common_exception::Result<RecordBatch> {
    let column_id_to_dfs_id = original_schema
        .to_leaf_column_ids()
        .iter()
        .enumerate()
        .map(|(dfs_id, column_id)| (*column_id, dfs_id))
        .collect::<HashMap<_, _>>();
    let mut projection_mask = Vec::with_capacity(column_chunks.len());
    let mut builder = RowGroupImplBuilder::new(
        num_rows,
        parquet_schema,
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
    let row_group = Box::new(builder.build());
    let field_levels = parquet_to_arrow_field_levels(
        parquet_schema,
        ProjectionMask::leaves(parquet_schema, projection_mask),
        Some(arrow_schema.fields()),
    )?;
    let mut record_reader = ParquetRecordBatchReader::try_new_with_row_groups(
        &field_levels,
        row_group.as_ref(),
        num_rows,
        selection,
    )?;
    let record = record_reader.next().unwrap()?;
    assert!(record_reader.next().is_none());
    Ok(record)
}
