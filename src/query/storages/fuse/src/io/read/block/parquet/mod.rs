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
mod adapter;
mod deserialize;
mod encode;
mod row_selection;

pub use adapter::RowGroupImplBuilder;
pub use deserialize::column_chunks_to_record_batch;
pub use deserialize::column_chunks_to_record_batch_with_codecs;
pub use deserialize::dictionary_hint_fields;
pub use encode::ColumnEncoding;
pub use row_selection::RowSelection;

use crate::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::io::FuseCaches;
use crate::io::read::block::block_reader_merge_io::DataItem;

impl BlockReader {
    pub fn deserialize_part(
        &self,
        part: &FuseBlockPartInfo,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
    ) -> databend_common_exception::Result<DataBlock> {
        self.deserialize_parquet_files(part.nums_rows, &part.data_files(), column_chunks, selection)
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
        self.deserialize_parquet_files(num_rows, &[file], column_chunks, selection)
    }

    /// Deserializes the column chunks a block read from `files`. Each chunk is decompressed
    /// with the codec of the file that stores its column.
    pub fn deserialize_parquet_files(
        &self,
        num_rows: usize,
        files: &[BlockDataFile<'_>],
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
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

        let has_selection = selection.is_some();
        let parquet_selection = selection.map(|s| s.selection.clone());
        let record_batch = column_chunks_to_record_batch_with_codecs(
            &self.original_schema,
            num_rows,
            &column_chunks,
            files.default_compression(),
            |column_id| files.compression_of(column_id),
            parquet_selection,
            self.parquet_hint_fields.as_ref(),
        )?;
        let mut entries = Vec::with_capacity(self.projected_schema.fields.len());
        let name_paths = column_name_paths(&self.projection, &self.original_schema);

        let caches = FuseCaches::instance();
        let array_cache = Some(&caches.column_arrays)
            .filter(|cache| self.policy.memory.writes() && !has_selection && cache.is_enabled());

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
                Some(DataItem::RawData(_)) => {
                    // get the deserialized arrow array, which may be a nested array
                    let arrow_array = column_by_name(&record_batch, &name_paths[i]);
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
fn column_name_paths(projection: &Projection, schema: &TableSchema) -> Vec<Vec<String>> {
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
