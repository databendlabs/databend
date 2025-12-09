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

use std::collections::BTreeMap;
use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_storages_common_table_meta::meta::BlockHLL;

use crate::io::write::stream::create_column_ndv_estimator;
use crate::io::write::stream::ColumnNDVEstimator;
use crate::io::write::stream::ColumnNDVEstimatorOps;

pub fn build_column_hlls(
    block: &DataBlock,
    ndv_columns_map: &BTreeMap<FieldIndex, TableField>,
) -> Result<Option<BlockHLL>> {
    let mut builder = BlockStatsBuilder::new(ndv_columns_map);
    builder.add_block(block)?;
    builder.finalize()
}

pub struct BlockStatsBuilder {
    builders: Vec<ColumnNDVBuilder>,
}

pub struct ColumnNDVBuilder {
    index: FieldIndex,
    field: TableField,
    pub builder: ColumnNDVEstimator,
}

impl BlockStatsBuilder {
    pub fn new(ndv_columns_map: &BTreeMap<FieldIndex, TableField>) -> BlockStatsBuilder {
        let mut builders = Vec::with_capacity(ndv_columns_map.len());
        for (index, field) in ndv_columns_map {
            let builder = create_column_ndv_estimator(&field.data_type().into());
            builders.push(ColumnNDVBuilder {
                index: *index,
                field: field.clone(),
                builder,
            });
        }
        BlockStatsBuilder { builders }
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        let mut keys_to_remove = vec![];
        for (index, column_builder) in self.builders.iter_mut().enumerate() {
            let entry = block.get_by_offset(column_builder.index);
            match entry {
                BlockEntry::Const(s, ..) => {
                    column_builder.builder.update_scalar(&s.as_ref());
                }
                BlockEntry::Column(col) => {
                    if col.check_large_string() {
                        keys_to_remove.push(index);
                        continue;
                    }
                    column_builder.builder.update_column(col);
                }
            }
        }

        // reverse sorting.
        keys_to_remove.sort_by(|a, b| b.cmp(a));
        for k in keys_to_remove {
            self.builders.remove(k);
        }
        Ok(())
    }

    pub fn peek_cols_ndv(&self) -> HashMap<ColumnId, usize> {
        self.builders
            .iter()
            .map(|item| (item.field.column_id(), item.builder.peek()))
            .collect()
    }

    pub fn finalize(self) -> Result<Option<BlockHLL>> {
        if self.builders.is_empty() {
            return Ok(None);
        }

        let mut column_hlls = HashMap::with_capacity(self.builders.len());
        for column_builder in self.builders {
            let column_id = column_builder.field.column_id();
            let hll = column_builder.builder.hll();
            column_hlls.insert(column_id, hll);
        }

        Ok(Some(column_hlls))
    }
}
