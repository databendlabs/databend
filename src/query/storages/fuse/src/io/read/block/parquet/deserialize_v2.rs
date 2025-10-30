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
use std::sync::Arc;
use bytes::Buf;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_parquet_reader_experimental as v2_reader;
use databend_common_storage::ColumnNode;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::SizedColumnArray;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use parquet2::metadata::Descriptor;
use v2_reader::calculate_parquet_max_levels;
use v2_reader::data_chunk_to_col_iter;
use v2_reader::from_table_field_type;

use super::BlockReader;
use crate::io::read::block::block_reader_merge_io::DataItem;

pub struct FieldDeserializationContext<'a> {
    pub(crate) column_metas: &'a HashMap<ColumnId, ColumnMeta>,
    pub(crate) column_chunks: &'a HashMap<ColumnId, DataItem<'a>>,
    pub(crate) num_rows: usize,
    pub(crate) compression: &'a Compression,
}

enum DeserializedColumn<'a> {
    FromCache(&'a Arc<SizedColumnArray>),
    Column((ColumnId, Column, usize)),
}

impl BlockReader {
    pub(crate) fn deserialize_v2(
        &self,
        block_path: &str,
        num_rows: usize,
        compression: &Compression,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
    ) -> Result<DataBlock> {
        if column_chunks.is_empty() {
            return self.build_default_values_block(num_rows);
        }

        let mut need_default_vals = Vec::with_capacity(self.project_column_nodes.len());
        let mut need_to_fill_default_val = false;
        let mut deserialized_column_arrays = Vec::with_capacity(self.projection.len());
        let field_deserialization_ctx = FieldDeserializationContext {
            column_metas,
            column_chunks: &column_chunks,
            num_rows,
            compression,
        };

        for column_node in &self.project_column_nodes {
            let deserialized_column = self
                .deserialize_field_v2(&field_deserialization_ctx, column_node)
                .map_err(|e| {
                    e.add_message(format!(
                        "failed to deserialize column: {:?}, location {} ",
                        column_node, block_path
                    ))
                })?;
            match deserialized_column {
                None => {
                    need_to_fill_default_val = true;
                    need_default_vals.push(true);
                }
                Some(v) => {
                    deserialized_column_arrays.push((v, column_node.table_field.data_type()));
                    need_default_vals.push(false);
                }
            }
        }

        let cache = if self.put_cache {
            CacheManager::instance().get_table_data_array_cache()
        } else {
            None
        };

        let mut block_entries = Vec::with_capacity(deserialized_column_arrays.len());
        for (col, table_data_type) in deserialized_column_arrays {
            // TODO we should cache deserialized data as Column (instead of arrow Array)
            // Converting arrow array to column may be expensive
            let entry = match col {
                DeserializedColumn::FromCache(arrow_array) => {
                    BlockEntry::Column(Column::from_arrow_rs(
                        arrow_array.0.clone(),
                        &(&table_data_type.clone()).into(),
                    )?)
                }
                DeserializedColumn::Column((column_id, col, size)) => {
                    if let Some(cache) = &cache {
                        let meta = column_metas.get(&column_id).unwrap();
                        let (offset, len) = meta.offset_length();
                        let key = TableDataCacheKey::new(block_path, column_id, offset, len);
                        let array = col.clone().into_arrow_rs();
                        cache.insert(key.into(), (array, size));
                    };
                    BlockEntry::Column(col)
                }
            };
            block_entries.push(entry);
        }

        // build data block
        let data_block = if !need_to_fill_default_val {
            assert_eq!(block_entries.len(), self.projected_schema.num_fields());
            DataBlock::new(block_entries, num_rows)
        } else {
            let mut default_vals = Vec::with_capacity(need_default_vals.len());
            for (i, need_default_val) in need_default_vals.iter().enumerate() {
                if !need_default_val {
                    default_vals.push(None);
                } else {
                    default_vals.push(Some(self.default_vals[i].clone()));
                }
            }

            create_with_opt_default_value(
                block_entries,
                &self.data_schema(),
                &default_vals,
                num_rows,
            )?
        };
        Ok(data_block)
    }

    fn deserialize_field_v2<'a>(
        &self,
        deserialization_context: &'a FieldDeserializationContext,
        column_node: &ColumnNode,
    ) -> Result<Option<DeserializedColumn<'a>>> {
        let is_nested = column_node.is_nested;

        if is_nested {
            unimplemented!("Nested type is not supported now");
        }

        let column_chunks = deserialization_context.column_chunks;
        let compression = deserialization_context.compression;

        let (max_def_level, max_rep_level) =
            calculate_parquet_max_levels(&column_node.table_field.data_type);

        let parquet_primitive_type = from_table_field_type(
            column_node.table_field.name.clone(),
            &column_node.table_field.data_type,
        );

        let column_descriptor = Descriptor {
            primitive_type: parquet_primitive_type,
            max_def_level,
            max_rep_level,
        };

        // Since we only support leaf column now
        let leaf_column_id = 0;
        let column_id = column_node.leaf_column_ids[leaf_column_id];

        let Some(column_meta) = deserialization_context.column_metas.get(&column_id) else {
            return Ok(None);
        };
        let Some(chunk) = column_chunks.get(&column_id) else {
            return Ok(None);
        };

        match chunk {
            DataItem::RawData(data) => {
                let field_uncompressed_size = data.len();
                let num_rows = deserialization_context.num_rows;
                let field_name = column_node.field.name().to_owned();

                let mut column_iter = data_chunk_to_col_iter(
                    column_meta,
                    data.chunk(),
                    num_rows,
                    &column_descriptor,
                    column_node.table_field.clone(),
                    compression,
                )?;

                let column = column_iter.next().transpose()?.ok_or_else(|| {
                    ErrorCode::StorageOther(format!("no array found for field {field_name}"))
                })?;

                // Since we deserialize all the rows of this column, the iterator should be drained
                assert!(column_iter.next().is_none());
                // Deserialized from raw bytes, and intended to be cached
                Ok(Some(DeserializedColumn::Column((
                    column_id,
                    column,
                    field_uncompressed_size,
                ))))
            }
            DataItem::ColumnArray(column_array) => {
                if is_nested {
                    return Err(ErrorCode::StorageOther(
                        "unexpected nested field: nested leaf field hits cached",
                    ));
                }
                // since it is not nested, this field contains only one column
                Ok(Some(DeserializedColumn::FromCache(column_array)))
            }
        }
    }
}

fn create_with_opt_default_value(
    block_entries: Vec<BlockEntry>,
    schema: &DataSchema,
    default_vals: &[Option<Scalar>],
    num_rows: usize,
) -> Result<DataBlock> {
    let schema_fields = schema.fields();
    let mut block_entries_iter = block_entries.into_iter();

    let mut entries = Vec::with_capacity(default_vals.len());
    for (i, default_val) in default_vals.iter().enumerate() {
        let field = &schema_fields[i];
        let data_type = field.data_type();

        let entry = match default_val {
            Some(default_val) => {
                BlockEntry::new_const_column(data_type.clone(), default_val.to_owned(), num_rows)
            }
            None => block_entries_iter
                .next()
                .expect("arrays should have enough elements"),
        };

        entries.push(entry);
    }

    Ok(DataBlock::new(entries, num_rows))
}
