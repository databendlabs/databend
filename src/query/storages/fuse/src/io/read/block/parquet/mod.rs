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
use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use databend_common_catalog::plan::Projection;
use databend_common_exception::ErrorCode;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterVisitor;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::Value;
use databend_common_expression::types::DataType;
use databend_common_expression::visitor::ValueVisitor;
use databend_storages_common_cache::CachedColumnArray;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_table_meta::meta::BlockDataFile;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
mod adapter;
mod deserialize;
mod dict_chunk;
mod encode;
mod page_header;
mod row_selection;

pub use adapter::RowGroupImplBuilder;
pub use deserialize::FieldLevelsCache;
pub use deserialize::ParquetReadSchema;
pub use deserialize::column_chunks_to_record_batch;
pub use deserialize::column_chunks_to_record_batch_with_codecs;
pub use deserialize::dictionary_hint_fields;
use dict_chunk::read_string_dictionary_chunk;
pub use encode::ColumnEncoding;
use page_header::dictionary_len;
use parquet::basic::Compression as ParquetCompression;
pub use row_selection::RowSelection;

use self::deserialize::column_chunks_record_reader;
use crate::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::io::FuseCaches;
use crate::io::read::block::block_reader_merge_io::DataItem;

impl BlockReader {
    /// A [`FieldLevelsCache`] for the blocks this reader reads, to pass along with each of them
    /// by a caller that reads many blocks.
    pub fn field_levels_cache(&self) -> FieldLevelsCache {
        FieldLevelsCache::new(self.parquet_read_schema.clone())
    }

    /// The projected non-nullable string columns kept as dictionaries whose chunk is
    /// dictionary encoded throughout, with a dictionary within the NDV ratio (as in
    /// [`ColumnEncoding::encode`]).
    fn dictionary_chunks(
        &self,
        num_rows: usize,
        column_chunks: &HashMap<ColumnId, DataItem>,
    ) -> HashSet<ColumnId> {
        self.projected_schema
            .fields
            .iter()
            .filter(|field| {
                *field.data_type() == TableDataType::String
                    && self.column_encodings.get(&field.column_id)
                        == Some(&ColumnEncoding::Dictionary)
                    && matches!(
                        column_chunks.get(&field.column_id),
                        Some(DataItem::RawData(chunk))
                            if dictionary_len(chunk).is_some_and(|len| {
                                len > 0
                                    && len as u64 * 100
                                        <= num_rows as u64 * self.dictionary_ndv_ratio
                            })
                    )
            })
            .map(|field| field.column_id)
            .collect()
    }

    pub fn deserialize_part(
        &self,
        part: &FuseBlockPartInfo,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
        field_levels: Option<&mut FieldLevelsCache>,
    ) -> databend_common_exception::Result<DataBlock> {
        self.deserialize_parquet_files(
            part.nums_rows,
            &part.data_files(),
            column_chunks,
            selection,
            field_levels,
        )
    }

    pub fn deserialize_parquet_chunks(
        &self,
        num_rows: usize,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        compression: &Compression,
        block_path: &str,
        selection: Option<&RowSelection>,
    ) -> databend_common_exception::Result<DataBlock> {
        let file = BlockDataFile {
            location: block_path,
            col_metas: column_metas,
            compression: *compression,
        };
        self.deserialize_parquet_files(num_rows, &[file], column_chunks, selection, None)
    }

    /// Deserializes the column chunks a block read from `files`. Each chunk is decompressed
    /// with the codec of the file that stores its column. `field_levels` is a cache from
    /// [`Self::field_levels_cache`], if the caller keeps one.
    pub fn deserialize_parquet_files(
        &self,
        num_rows: usize,
        files: &[BlockDataFile<'_>],
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
        field_levels: Option<&mut FieldLevelsCache>,
    ) -> databend_common_exception::Result<DataBlock> {
        let files = DataFileLookup::new(files);
        let result_rows = selection.map(|s| s.selected_rows).unwrap_or(num_rows);
        // If projection is empty, return a DataBlock with the appropriate row count but no columns
        if self.projected_schema.fields.is_empty() {
            return Ok(DataBlock::empty_with_rows(result_rows));
        }

        if result_rows == 0 {
            return Ok(DataBlock::empty_with_schema(&self.data_schema()));
        }

        // Dictionary encoded chunks of the string columns kept as dictionaries are read
        // without the arrow reader, see `dict_chunk`.
        let dictionary_chunks = self.dictionary_chunks(num_rows, &column_chunks);
        let arrow_chunks;
        let arrow_chunks = if dictionary_chunks.is_empty() {
            &column_chunks
        } else {
            arrow_chunks = column_chunks
                .iter()
                .filter(|(column_id, _)| !dictionary_chunks.contains(column_id))
                .map(|(column_id, item)| (*column_id, item.clone()))
                .collect::<HashMap<_, _>>();
            &arrow_chunks
        };
        let record_batch = column_chunks_to_record_batch_with_codecs(
            &self.original_schema,
            &self.parquet_read_schema,
            field_levels,
            num_rows,
            arrow_chunks,
            files.default_compression(),
            |column_id| files.compression_of(column_id),
            selection.map(|s| s.selection.clone()),
        )?;
        self.record_batch_to_block(
            &record_batch,
            result_rows,
            &files,
            &column_chunks,
            &dictionary_chunks,
            num_rows,
            selection,
            true,
        )
    }

    /// Whether [`Self::parquet_batches`] can decode `column_chunks` in batches rather than
    /// whole: cache entries and encodings describe the whole block, not a batch.
    ///
    /// Callers check it once per block, as the column array cache may be enabled by another
    /// session at any time.
    pub(crate) fn can_parquet_batches(&self, column_chunks: &HashMap<ColumnId, DataItem>) -> bool {
        !(self.policy.memory.writes() && FuseCaches::instance().column_arrays.is_enabled())
            && self.column_encodings.is_empty()
            && !column_chunks.is_empty()
            && !column_chunks
                .values()
                .any(|item| matches!(item, DataItem::ColumnArray(_)))
    }

    /// Decodes the column chunks of a block in batches of at most `batch_rows` rows.
    ///
    /// The returned decoder owns the column chunks, so batches can be pulled after the read
    /// result is gone: each pull decompresses only the pages of that batch. The batches
    /// never fill the column array cache, whose entries hold whole columns.
    ///
    /// The caller must have checked [`Self::can_parquet_batches`] on `column_chunks`.
    pub(crate) fn parquet_batches(
        self: &Arc<Self>,
        part: &FuseBlockPartInfo,
        column_chunks: &HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
        batch_rows: usize,
        field_levels: &mut FieldLevelsCache,
    ) -> databend_common_exception::Result<ParquetBatches> {
        debug_assert!(self.column_encodings.is_empty());
        let data_files = part.data_files();
        let files = DataFileLookup::new(&data_files);
        // Cap the reader batch size by the selected/total row count so sparse pushdowns
        // do not over-allocate output column buffers retained downstream.
        let reader_batch_rows = selection
            .map_or(part.nums_rows, |s| s.selected_rows)
            .min(batch_rows);
        let reader = column_chunks_record_reader(
            &self.original_schema,
            &self.parquet_read_schema,
            Some(field_levels),
            part.nums_rows,
            column_chunks,
            files.default_compression(),
            |column_id| files.compression_of(column_id),
            selection.map(|s| s.selection.clone()),
            reader_batch_rows,
        )?;
        let column_chunks = column_chunks
            .iter()
            .map(|(column_id, item)| match item {
                DataItem::RawData(buffer) => (*column_id, DataItem::RawData(buffer.clone())),
                DataItem::ColumnArray(_) => unreachable!("checked by can_parquet_batches"),
            })
            .collect();
        Ok(ParquetBatches {
            block_reader: self.clone(),
            column_chunks,
            reader,
        })
    }

    /// `dictionary_chunks` are decoded from `column_chunks` instead of `record_batch`; they hold
    /// `num_rows` rows before `selection`.
    #[allow(clippy::too_many_arguments)]
    fn record_batch_to_block(
        &self,
        record_batch: &RecordBatch,
        result_rows: usize,
        files: &DataFileLookup,
        column_chunks: &HashMap<ColumnId, DataItem>,
        dictionary_chunks: &HashSet<ColumnId>,
        num_rows: usize,
        selection: Option<&RowSelection>,
        whole_block: bool,
    ) -> databend_common_exception::Result<DataBlock> {
        let mut entries = Vec::with_capacity(self.projected_schema.fields.len());
        let caches = FuseCaches::instance();
        // Only the columns of a whole block, all rows, are cached.
        let array_cache = Some(&caches.column_arrays).filter(|cache| {
            whole_block && self.policy.memory.writes() && selection.is_none() && cache.is_enabled()
        });

        for ((i, field), column_node) in self
            .projected_schema
            .fields
            .iter()
            .enumerate()
            .zip(self.project_column_nodes.iter())
        {
            let data_type: DataType = field.data_type().into();
            let encoding = self.column_encodings.get(&field.column_id).copied();

            // NOTE, there is something tricky here:
            // - `column_chunks` always contains data of leaf columns
            // - here we may processing a nested type field
            // - But, even if the field being processed is a field with multiple leaf columns
            //    `column_chunks.get(&field.column_id)` will still return Some(DataItem::_)[^1],
            //    even if we are getting data from `column_chunks` using a non-leaf
            //    `column_id` of `projected_schema.fields`
            //
            //   [^1]: Except in the current block, there is no data stored for the
            //         corresponding field, and a default value has been declared for
            //         the corresponding field.
            //
            //  Yes, it is too obscure, we need to polish it later.

            let value = match column_chunks.get(&field.column_id) {
                Some(DataItem::RawData(chunk)) if dictionary_chunks.contains(&field.column_id) => {
                    let compression =
                        ParquetCompression::from(files.compression_of(field.column_id));
                    let column =
                        read_string_dictionary_chunk(chunk.clone(), compression, num_rows)?;
                    if let Some(cache) = array_cache {
                        let file = files.file_of(field.column_id).unwrap();
                        let meta = file.col_metas.get(&field.column_id).unwrap();
                        let (offset, len) = meta.offset_length();
                        let cache_key = TableDataCacheKey::new(
                            file.location,
                            field.column_id,
                            offset,
                            len,
                            &ColumnEncoding::cache_type_key(encoding, &data_type),
                        );
                        let memory_size = column.memory_size(false);
                        cache.fill(
                            cache_key.as_ref(),
                            (CachedColumnArray::Column(column.clone()), memory_size),
                        );
                    }
                    // The whole chunk is decoded: filter it as the arrow reader would.
                    let mut value = Value::Column(column);
                    if let Some(selection) = selection {
                        let mut filter_visitor = FilterVisitor::new(&selection.bitmap);
                        filter_visitor.visit_value(value)?;
                        value = filter_visitor.take_result().unwrap();
                    }
                    value
                }
                Some(DataItem::RawData(_)) => {
                    // get the deserialized arrow array, which may be a nested array
                    let arrow_array = column_by_name(record_batch, &self.parquet_name_paths[i]);
                    // Nested columns are neither cached nor encoded.
                    let cache = array_cache.filter(|_| !column_node.is_nested);
                    let cache_key = || {
                        let file = files.file_of(field.column_id).unwrap();
                        let meta = file.col_metas.get(&field.column_id).unwrap();
                        let (offset, len) = meta.offset_length();
                        TableDataCacheKey::new(
                            file.location,
                            field.column_id,
                            offset,
                            len,
                            &ColumnEncoding::cache_type_key(encoding, &data_type),
                        )
                    };
                    match encoding {
                        Some(encoding) => {
                            let column = encoding.encode(
                                arrow_array,
                                &data_type,
                                self.dictionary_ndv_ratio,
                                self.build_encodings,
                            )?;
                            if let Some(cache) = cache {
                                let memory_size = column.memory_size(false);
                                cache.fill(
                                    cache_key().as_ref(),
                                    (CachedColumnArray::Column(column.clone()), memory_size),
                                );
                            }
                            Value::Column(column)
                        }
                        None => {
                            if let Some(cache) = cache {
                                let array_memory_size = arrow_array.get_array_memory_size();
                                cache.fill(
                                    cache_key().as_ref(),
                                    (arrow_array.clone().into(), array_memory_size),
                                );
                            }
                            Value::from_arrow_rs(arrow_array, &data_type)?
                        }
                    }
                }
                Some(DataItem::ColumnArray(cached)) => {
                    if column_node.is_nested {
                        // a defensive check, should never happen
                        return Err(ErrorCode::StorageOther(
                            "unexpected nested field: nested leaf field hits cached",
                        ));
                    }
                    let mut value = match &cached.0 {
                        CachedColumnArray::Arrow(array) => {
                            Value::from_arrow_rs(array.clone(), &data_type)?
                        }
                        CachedColumnArray::Column(column) => Value::Column(column.clone()),
                    };
                    if let Some(selection) = selection {
                        let mut filter_visitor = FilterVisitor::new(&selection.bitmap);
                        filter_visitor.visit_value(value)?;
                        value = filter_visitor.take_result().unwrap();
                    }
                    value
                }
                None => Value::Scalar(self.default_vals[i].clone()),
            };
            entries.push(BlockEntry::new(value, || (data_type, result_rows)));
        }
        Ok(DataBlock::new(entries, result_rows))
    }
}

/// Batch decoder of one block from [`BlockReader::parquet_batches`].
pub(crate) struct ParquetBatches {
    block_reader: Arc<BlockReader>,
    /// The chunks the reader decodes: they tell present columns from defaulted ones.
    column_chunks: HashMap<ColumnId, DataItem<'static>>,
    reader: ParquetRecordBatchReader,
}

impl ParquetBatches {
    /// Decodes the next batch of the block `part`, the block the decoder was built for.
    pub(crate) fn next_block(
        &mut self,
        part: &FuseBlockPartInfo,
    ) -> Option<databend_common_exception::Result<DataBlock>> {
        let batch = match self.reader.next()? {
            Ok(batch) => batch,
            Err(err) => return Some(Err(err.into())),
        };
        let data_files = part.data_files();
        Some(self.block_reader.record_batch_to_block(
            &batch,
            batch.num_rows(),
            &DataFileLookup::new(&data_files),
            &self.column_chunks,
            // Without encodings no chunk is read as a dictionary chunk.
            &HashSet::new(),
            batch.num_rows(),
            None,
            false,
        ))
    }
}

/// Finds the file of a block that stores a column.
struct DataFileLookup<'a> {
    files: &'a [BlockDataFile<'a>],
    /// Absent when the block has at most one file.
    file_of_column: Option<HashMap<ColumnId, usize>>,
}

impl<'a> DataFileLookup<'a> {
    fn new(files: &'a [BlockDataFile<'a>]) -> Self {
        let file_of_column = (files.len() > 1).then(|| {
            files
                .iter()
                .enumerate()
                .flat_map(|(i, file)| file.col_metas.keys().map(move |id| (*id, i)))
                .collect()
        });
        Self {
            files,
            file_of_column,
        }
    }

    fn file_of(&self, column_id: ColumnId) -> Option<&BlockDataFile<'a>> {
        match &self.file_of_column {
            None => self.files.first(),
            Some(files) => files.get(&column_id).map(|i| &self.files[*i]),
        }
    }

    fn compression_of(&self, column_id: ColumnId) -> Compression {
        self.file_of(column_id)
            .map_or_else(|| self.default_compression(), |file| file.compression)
    }

    /// The codec of the columns without a chunk. Nothing is decompressed with it.
    fn default_compression(&self) -> Compression {
        self.files
            .first()
            .map_or(Compression::None, |file| file.compression)
    }
}

fn column_by_name(record_batch: &RecordBatch, names: &[String]) -> ArrayRef {
    let mut array = record_batch.column_by_name(&names[0]).unwrap().clone();
    if names.len() > 1 {
        for name in &names[1..] {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array = struct_array.column_by_name(name).unwrap().clone();
        }
    }
    array
}

// This function assumes that projection is valid, isn't responsible for checking it.
pub(crate) fn column_name_paths(projection: &Projection, schema: &TableSchema) -> Vec<Vec<String>> {
    match projection {
        Projection::Columns(field_indices) => field_indices
            .iter()
            .map(|i| vec![schema.fields[*i].name().to_string()])
            .collect(),
        Projection::InnerColumns(path_indices) => {
            let mut name_paths = Vec::with_capacity(path_indices.len());
            for index_path in path_indices.values() {
                let mut name_path = Vec::with_capacity(index_path.len());
                let first_index = index_path[0];
                name_path.push(schema.fields[first_index].name().to_string());
                let mut idx = 1;
                let mut ty = schema.fields[first_index].data_type().clone();
                while idx < index_path.len() {
                    match ty.remove_nullable() {
                        TableDataType::Tuple {
                            fields_name,
                            fields_type,
                        } => {
                            let next_index = index_path[idx];
                            name_path.push(fields_name[next_index].clone());
                            ty = fields_type[next_index].clone();
                        }
                        _ => unreachable!(),
                    }
                    idx += 1;
                }
                name_paths.push(name_path);
            }
            name_paths
        }
    }
}
