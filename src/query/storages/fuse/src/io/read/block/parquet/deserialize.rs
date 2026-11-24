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
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::FieldRef;
use arrow_schema::Fields;
use arrow_schema::Schema;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::Compression;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::basic::Compression as ParquetCompression;
use parquet::schema::types::SchemaDescPtr;

use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::io::read::block::parquet::adapter::RowGroupImplBuilder;

/// The sorted Parquet leaves read from a block, and the positions of the dictionary hints
/// dropped for its column chunks without a dictionary page.
type FieldLevelsKey = (Vec<usize>, Vec<usize>);

const MAX_CACHED_FIELD_LEVEL_LAYOUTS: usize = 16;

/// What reading Parquet column chunks of a table schema needs besides the chunks: the Arrow
/// and Parquet forms of the schema, the Parquet leaf of every column id, and the fields to
/// read the columns as.
///
/// All of it depends on the schema only, not on the block, so a reader builds it once and
/// reads every block with it. Converting a wide schema per block cost more than decoding the
/// columns of a small block.
pub struct ParquetReadSchema {
    arrow_schema: Schema,
    parquet_schema: SchemaDescPtr,
    column_id_to_dfs_id: HashMap<ColumnId, usize>,
    /// Arrow fields the columns are read as if they differ from `arrow_schema`, see
    /// [`dictionary_hint_fields`].
    hint_fields: Option<Fields>,
}

impl ParquetReadSchema {
    pub fn try_new(
        original_schema: &TableSchema,
        hint_fields: Option<Fields>,
    ) -> databend_common_exception::Result<Self> {
        let arrow_schema = Schema::from(original_schema);
        let parquet_schema = Arc::new(ArrowSchemaConverter::new().convert(&arrow_schema)?);
        let column_id_to_dfs_id = original_schema
            .to_leaf_column_ids()
            .iter()
            .enumerate()
            .map(|(dfs_id, column_id)| (*column_id, dfs_id))
            .collect();
        Ok(Self {
            arrow_schema,
            parquet_schema,
            column_id_to_dfs_id,
            hint_fields,
        })
    }

    fn field_levels(&self, key: &FieldLevelsKey) -> databend_common_exception::Result<FieldLevels> {
        let base_fields = self
            .hint_fields
            .as_ref()
            .unwrap_or(self.arrow_schema.fields());
        let adjusted_fields = (!key.1.is_empty()).then(|| {
            let mut fields = base_fields.to_vec();
            for &index in &key.1 {
                fields[index] = self.arrow_schema.fields()[index].clone();
            }
            Fields::from(fields)
        });
        Ok(parquet_to_arrow_field_levels(
            &self.parquet_schema,
            ProjectionMask::leaves(&self.parquet_schema, key.0.iter().copied()),
            Some(adjusted_fields.as_ref().unwrap_or(base_fields)),
        )?)
    }
}

/// Field levels computed with one [`ParquetReadSchema`], by the leaves read and the dictionary
/// hints dropped. A reading processor owns one per reader and passes it along with every block.
/// Once the cache is full, it reuses existing layouts but does not retain new ones.
pub struct FieldLevelsCache {
    read_schema: Arc<ParquetReadSchema>,
    levels: HashMap<FieldLevelsKey, FieldLevels>,
}

impl FieldLevelsCache {
    pub fn new(read_schema: Arc<ParquetReadSchema>) -> Self {
        Self {
            read_schema,
            levels: HashMap::new(),
        }
    }

    /// Whether this cache holds field levels computed with `read_schema`.
    fn is_for(&self, read_schema: &ParquetReadSchema) -> bool {
        std::ptr::eq(Arc::as_ptr(&self.read_schema), read_schema)
    }
}

/// Arrow fields of `original_schema` with the top-level string fields in `dictionary_columns`
/// typed `Dictionary(UInt32, Utf8)`. Passed as the hint of [`column_chunks_to_record_batch`],
/// it makes the Parquet reader preserve the dictionary encoding of these column chunks instead
/// of expanding it. `None` if there is no such field.
pub fn dictionary_hint_fields(
    original_schema: &TableSchema,
    dictionary_columns: impl IntoIterator<Item = ColumnId>,
) -> Option<Fields> {
    let mut fields: Option<Vec<FieldRef>> = None;
    for column_id in dictionary_columns {
        // The type check guards against a nested field sharing the column id of its leaf.
        let Some(index) = original_schema.fields().iter().position(|field| {
            field.column_id() == column_id && *field.data_type() == TableDataType::String
        }) else {
            continue;
        };
        let fields = fields.get_or_insert_with(|| {
            Schema::from(original_schema)
                .fields()
                .iter()
                .cloned()
                .collect()
        });
        let dictionary = ArrowDataType::Dictionary(
            Box::new(ArrowDataType::UInt32),
            Box::new(ArrowDataType::Utf8),
        );
        fields[index] = Arc::new(fields[index].as_ref().clone().with_data_type(dictionary));
    }
    fields.map(Fields::from)
}

/// The returned record batch contains all deserialized columns in the same nested structure as the original schema.
///
/// `hint_fields` overrides the Arrow fields the columns are read as, see
/// [`dictionary_hint_fields`]; by default they are the fields of `original_schema`. A dictionary
/// hint only applies to column chunks that are dictionary encoded: the others are read as their
/// regular type, as the reader would otherwise rebuild a dictionary row by row.
pub fn column_chunks_to_record_batch(
    original_schema: &TableSchema,
    num_rows: usize,
    column_chunks: &HashMap<ColumnId, DataItem>,
    compression: &Compression,
    selection: Option<RowSelection>,
    hint_fields: Option<&Fields>,
) -> databend_common_exception::Result<RecordBatch> {
    let read_schema = ParquetReadSchema::try_new(original_schema, hint_fields.cloned())?;
    column_chunks_to_record_batch_with_codecs(
        original_schema,
        &read_schema,
        None,
        num_rows,
        column_chunks,
        *compression,
        |_| *compression,
        selection,
    )
}

/// Like [`column_chunks_to_record_batch`], but with the [`ParquetReadSchema`] of
/// `original_schema` built by the caller, optionally a [`FieldLevelsCache`] of it, and each
/// column chunk decompressed with the codec `compression_of` returns for its leaf column id.
/// `default_compression` describes the columns that have no chunk.
#[allow(clippy::too_many_arguments)]
pub fn column_chunks_to_record_batch_with_codecs(
    original_schema: &TableSchema,
    read_schema: &ParquetReadSchema,
    field_levels: Option<&mut FieldLevelsCache>,
    num_rows: usize,
    column_chunks: &HashMap<ColumnId, DataItem>,
    default_compression: Compression,
    compression_of: impl Fn(ColumnId) -> Compression,
    selection: Option<RowSelection>,
) -> databend_common_exception::Result<RecordBatch> {
    // The reader uses batch_size to preallocate column buffers retained by the output.
    let batch_size = selection.as_ref().map_or(num_rows, RowSelection::row_count);
    let mut record_reader = column_chunks_record_reader(
        original_schema,
        read_schema,
        field_levels,
        num_rows,
        column_chunks,
        default_compression,
        compression_of,
        selection,
        batch_size,
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

#[allow(clippy::too_many_arguments)]
pub(super) fn column_chunks_record_reader(
    original_schema: &TableSchema,
    read_schema: &ParquetReadSchema,
    field_levels: Option<&mut FieldLevelsCache>,
    num_rows: usize,
    column_chunks: &HashMap<ColumnId, DataItem>,
    default_compression: Compression,
    compression_of: impl Fn(ColumnId) -> Compression,
    selection: Option<RowSelection>,
    batch_size: usize,
) -> databend_common_exception::Result<ParquetRecordBatchReader> {
    let mut projection_mask = Vec::with_capacity(column_chunks.len());
    let mut builder = RowGroupImplBuilder::new(
        num_rows,
        &read_schema.parquet_schema,
        ParquetCompression::from(default_compression),
    );
    for (column_id, data_item) in column_chunks.iter() {
        match data_item {
            DataItem::RawData(bytes) => {
                let dfs_id = read_schema.column_id_to_dfs_id[column_id];
                projection_mask.push(dfs_id);
                builder.add_column_chunk_with_compression(
                    dfs_id,
                    bytes.clone(),
                    ParquetCompression::from(compression_of(*column_id)),
                );
            }
            DataItem::ColumnArray(_) => {}
        }
    }
    projection_mask.sort_unstable();

    // Positions of the dictionary hints to drop for this block.
    let mut plain_hints = Vec::new();
    if let Some(hint_fields) = &read_schema.hint_fields {
        for (index, field) in hint_fields.iter().enumerate() {
            let column_id = original_schema.fields()[index].column_id();
            if matches!(field.data_type(), ArrowDataType::Dictionary(..))
                && read_schema
                    .column_id_to_dfs_id
                    .get(&column_id)
                    // Whether a column chunk is dictionary encoded was decided when writing it,
                    // from its distinct values (dictionary disabled for high NDV, or the
                    // arrow-rs writer falling back to plain once the dictionary page is too
                    // large), and shows as a leading dictionary page. Its header tells whether
                    // reading as a dictionary pays.
                    .is_some_and(|dfs_id| !builder.starts_with_dictionary_page(*dfs_id))
            {
                plain_hints.push(index);
            }
        }
    }

    let row_group = Box::new(builder.build());
    let key = (projection_mask, plain_hints);
    let cache = field_levels.filter(|cache| {
        debug_assert!(
            cache.is_for(read_schema),
            "field levels of another read schema"
        );
        cache.is_for(read_schema)
    });
    let computed;
    let field_levels = match cache {
        Some(cache) if cache.levels.len() < MAX_CACHED_FIELD_LEVEL_LAYOUTS => {
            match cache.levels.entry(key) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let levels = read_schema.field_levels(entry.key())?;
                    entry.insert(levels)
                }
            }
        }
        cache => match cache.and_then(|cache| cache.levels.get(&key)) {
            Some(levels) => levels,
            None => {
                computed = read_schema.field_levels(&key)?;
                &computed
            }
        },
    };
    Ok(ParquetRecordBatchReader::try_new_with_row_groups(
        field_levels,
        row_group.as_ref(),
        batch_size,
        selection,
    )?)
}

#[cfg(test)]
mod tests {
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
                None,
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

    /// A dictionary hint keeps the dictionary of a dictionary encoded column chunk, and reads a
    /// column chunk without a dictionary page as its regular type, also when one
    /// [`FieldLevelsCache`] serves chunks of both kinds one after the other.
    #[test]
    fn test_dictionary_hint_follows_column_chunk() -> databend_common_exception::Result<()> {
        let num_rows = 1000;
        let schema = TableSchema::new(vec![TableField::new("s", TableDataType::String)]);
        let arrow_schema = Arc::new(Schema::from(&schema));
        let values = (0..num_rows)
            .map(|i| format!("v{}", i % 3))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            arrow_array::StringViewArray::from(values.clone()),
        )])?;
        let column_id = schema.field(0).column_id();
        let hint = dictionary_hint_fields(&schema, [column_id]).unwrap();
        let read_schema = Arc::new(ParquetReadSchema::try_new(&schema, Some(hint))?);
        let mut field_levels = FieldLevelsCache::new(read_schema.clone());

        for dictionary_enabled in [true, false, true] {
            let properties = WriterProperties::builder()
                .set_dictionary_enabled(dictionary_enabled)
                .build();
            let mut bytes = Vec::new();
            let mut writer =
                ArrowWriter::try_new(&mut bytes, arrow_schema.clone(), Some(properties))?;
            writer.write(&batch)?;
            let metadata = writer.close()?;
            let (offset, length) = metadata.row_group(0).column(0).byte_range();
            let bytes = Bytes::from(bytes);
            let column_chunks = HashMap::from([(
                column_id,
                DataItem::RawData(Buffer::from(
                    bytes.slice(offset as usize..(offset + length) as usize),
                )),
            )]);

            let record = column_chunks_to_record_batch_with_codecs(
                &schema,
                &read_schema,
                Some(&mut field_levels),
                num_rows,
                &column_chunks,
                Compression::None,
                |_| Compression::None,
                None,
            )?;
            let column = record.column(0);
            assert_eq!(
                matches!(column.data_type(), ArrowDataType::Dictionary(..)),
                dictionary_enabled,
                "dictionary_enabled: {dictionary_enabled}"
            );
            let decoded = databend_common_expression::Column::from_arrow_rs(
                column.clone(),
                &databend_common_expression::types::DataType::String,
            )?;
            assert_eq!(
                decoded,
                <databend_common_expression::types::StringType as databend_common_expression::FromData<_>>::from_data(values.clone())
            );
        }
        Ok(())
    }
}
