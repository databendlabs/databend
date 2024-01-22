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

use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use databend_common_exception::ErrorCode;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_cache_manager::CacheManager;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;

mod adapter;
mod deserialize;

use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::io::read::block::parquet::deserialize::deserialize_column_chunks;
use crate::io::BlockReader;
impl BlockReader {
    pub(crate) fn deserialize_column_chunks_1(
        &self,
        num_rows: usize,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        compression: &Compression,
        block_path: &str,
    ) -> databend_common_exception::Result<DataBlock> {
        if column_chunks.is_empty() {
            return self.build_default_values_block(num_rows);
        }
        let record_batch = deserialize_column_chunks(
            &self.original_schema,
            num_rows,
            &column_chunks,
            compression,
        )?;
        let mut columns = Vec::with_capacity(self.projected_schema.fields.len());
        for (i, field) in self.projected_schema.fields.iter().enumerate() {
            let data_type = field.data_type().into();
            let leaf_column_ids = field.leaf_column_ids();
            let value = match column_chunks.get(&field.column_id) {
                Some(DataItem::RawData(data)) => {
                    let arrow_array = column_by_name(&record_batch, &field.name);
                    let arrow2_array: Box<dyn databend_common_arrow::arrow::array::Array> =
                        arrow_array.into();
                    if self.put_cache && leaf_column_ids.len() == 1 {
                        if let Some(cache) = CacheManager::instance().get_table_data_array_cache() {
                            let meta = column_metas.get(&field.column_id).unwrap();
                            let (offset, len) = meta.offset_length();
                            let key =
                                TableDataCacheKey::new(block_path, field.column_id, offset, len);
                            cache.put(key.into(), Arc::new((arrow2_array.clone(), data.len())))
                        }
                    }
                    Value::Column(Column::from_arrow(arrow2_array.as_ref(), &data_type)?)
                }
                Some(DataItem::ColumnArray(cached)) => {
                    if leaf_column_ids.len() != 1 {
                        return Err(ErrorCode::StorageOther(
                            "unexpected nested field: nested leaf field hits cached",
                        ));
                    }
                    Value::Column(Column::from_arrow(cached.0.as_ref(), &data_type)?)
                }
                None => Value::Scalar(self.default_vals[i].clone()),
            };
            columns.push(BlockEntry::new(data_type, value));
        }
        Ok(DataBlock::new(columns, num_rows))
    }
}

fn column_by_name(record_batch: &RecordBatch, name: &str) -> ArrayRef {
    let arrow_array = record_batch
        .column_by_name(name)
        .cloned()
        .unwrap_or_else(|| {
            let names = name.split(':').collect::<Vec<_>>();
            let mut array = record_batch.column_by_name(names[0]).unwrap().clone();
            for name in &names[1..] {
                let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                array = struct_array.column_by_name(name).unwrap().clone();
            }
            array
        });
    arrow_array
}
