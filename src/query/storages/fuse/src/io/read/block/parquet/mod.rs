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

use arrow_array::StructArray;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
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
        _column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        compression: &Compression,
        _block_path: &str,
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
            let value = match column_chunks.get(&field.column_id) {
                Some(DataItem::RawData(_)) => {
                    let arrow_array = record_batch
                        .column_by_name(&field.name)
                        .cloned()
                        .unwrap_or_else(|| {
                            let names = field.name.split(':').collect::<Vec<_>>();
                            let mut array = record_batch.column_by_name(names[0]).unwrap().clone();
                            for name in &names[1..] {
                                let struct_array =
                                    array.as_any().downcast_ref::<StructArray>().unwrap();
                                array = struct_array.column_by_name(name).unwrap().clone();
                            }
                            array
                        });
                    let arrow2_array: Box<dyn databend_common_arrow::arrow::array::Array> =
                        arrow_array.into();
                    Value::Column(Column::from_arrow(arrow2_array.as_ref(), &data_type)?)
                }
                Some(DataItem::ColumnArray(cached)) => {
                    Value::Column(Column::from_arrow(cached.0.as_ref(), &data_type)?)
                }
                None => Value::Scalar(self.default_vals[i].clone()),
            };
            columns.push(BlockEntry::new(data_type, value));
        }
        Ok(DataBlock::new(columns, num_rows))
    }
}
