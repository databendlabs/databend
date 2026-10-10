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

use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use databend_common_catalog::plan::Projection;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::Value;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
mod adapter;
mod deserialize;
mod row_selection;

pub use adapter::RowGroupImplBuilder;
pub(crate) use deserialize::ArrayCacheContext;
pub use deserialize::column_chunks_to_record_batch;
pub(crate) use deserialize::deserialize_column_chunks;
pub use row_selection::RowSelection;

use crate::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::io::read::block::block_reader_merge_io::DataItem;

impl BlockReader {
    pub fn deserialize_part(
        &self,
        part: &FuseBlockPartInfo,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
    ) -> databend_common_exception::Result<DataBlock> {
        self.deserialize_part_with_num_rows(part, part.nums_rows, column_chunks, selection)
    }

    /// Like [`deserialize_part`], but with an explicit row count. Used by sparse-granule-index
    /// narrowed reads, where the reconstructed partial column chunks contain fewer rows than the
    /// block's `nums_rows`.
    pub fn deserialize_part_with_num_rows(
        &self,
        part: &FuseBlockPartInfo,
        num_rows: usize,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
    ) -> databend_common_exception::Result<DataBlock> {
        self.deserialize_parquet_chunks_inner(
            num_rows,
            &part.columns_meta,
            column_chunks,
            &part.compression,
            &part.location,
            selection,
            num_rows == part.nums_rows,
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
        self.deserialize_parquet_chunks_inner(
            num_rows,
            column_metas,
            column_chunks,
            compression,
            block_path,
            selection,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn deserialize_parquet_chunks_inner(
        &self,
        num_rows: usize,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        compression: &Compression,
        block_path: &str,
        selection: Option<&RowSelection>,
        complete_column_chunks: bool,
    ) -> databend_common_exception::Result<DataBlock> {
        let result_rows = selection.map(|s| s.selected_rows).unwrap_or(num_rows);
        // If projection is empty, return a DataBlock with the appropriate row count but no columns
        if self.projected_schema.fields.is_empty() {
            return Ok(DataBlock::empty_with_rows(result_rows));
        }

        if result_rows == 0 {
            return Ok(DataBlock::empty_with_schema(&self.data_schema()));
        }

        let array_cache = match self.put_cache {
            true => CacheManager::instance().get_table_data_array_cache(),
            false => None,
        };
        let record_batch = deserialize_column_chunks(
            &self.original_schema,
            num_rows,
            &column_chunks,
            compression,
            selection.map(|s| s.selection.clone()),
            array_cache.as_ref().map(|cache| ArrayCacheContext {
                cache,
                location: block_path,
                column_metas,
                complete_column_chunks,
            }),
        )?;
        let mut entries = Vec::with_capacity(self.projected_schema.fields.len());
        let name_paths = column_name_paths(&self.projection, &self.original_schema);

        for (i, field) in self.projected_schema.fields.iter().enumerate() {
            let data_type = field.data_type().into();

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

            let data_item = column_chunks.get(&field.column_id);
            let value = match data_item {
                Some(_) => {
                    Value::from_arrow_rs(column_by_name(&record_batch, &name_paths[i]), &data_type)?
                }
                None => Value::Scalar(self.default_vals[i].clone()),
            };
            entries.push(BlockEntry::new(value, || (data_type, result_rows)));
        }
        Ok(DataBlock::new(entries, result_rows))
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
