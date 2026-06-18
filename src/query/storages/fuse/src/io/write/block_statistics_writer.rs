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
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ColumnTopN;

use crate::io::write::stream::ColumnNDVEstimator;
use crate::io::write::stream::ColumnNDVEstimatorOps;
use crate::io::write::stream::create_column_ndv_estimator;

pub fn build_column_hlls(
    block: &DataBlock,
    ndv_columns_map: &BTreeMap<FieldIndex, TableField>,
) -> Result<Option<BlockHLL>> {
    let mut builder = BlockStatsBuilder::new(ndv_columns_map, None);
    builder.add_block(block)?;
    builder.finalize()
}

pub struct BlockStatsBuilder {
    builders: Vec<ColumnNDVBuilder>,
}

pub struct ColumnNDVBuilder {
    index: FieldIndex,
    field: TableField,
    pub hll: Option<ColumnNDVEstimator>,
    pub top_n: Option<(ColumnTopN, usize)>,
}

impl BlockStatsBuilder {
    pub fn new(
        ndv_columns_map: &BTreeMap<FieldIndex, TableField>,
        top_n: Option<(&BTreeMap<FieldIndex, TableField>, usize)>,
    ) -> BlockStatsBuilder {
        let mut builders =
            Vec::with_capacity(ndv_columns_map.len() + top_n.map_or(0, |(v, _)| v.len()));

        for (index, field) in ndv_columns_map {
            let column_top_n = top_n
                .and_then(|(columns, size)| columns.contains_key(index).then_some(size))
                .map(|size| (ColumnTopN::default(), size));
            builders.push(ColumnNDVBuilder {
                index: *index,
                field: field.clone(),
                hll: Some(create_column_ndv_estimator(&field.data_type().into())),
                top_n: column_top_n,
            });
        }

        if let Some((top_n_columns_map, top_n_size)) = top_n {
            for (index, field) in top_n_columns_map {
                if ndv_columns_map.contains_key(index) {
                    continue;
                }
                builders.push(ColumnNDVBuilder {
                    index: *index,
                    field: field.clone(),
                    hll: None,
                    top_n: Some((ColumnTopN::default(), top_n_size)),
                });
            }
        }

        BlockStatsBuilder { builders }
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        let mut keys_to_remove = vec![];
        for (index, column_builder) in self.builders.iter_mut().enumerate() {
            let entry = block.get_by_offset(column_builder.index);
            match entry {
                BlockEntry::Const(s, _, num_rows) => {
                    if let Some(hll) = &mut column_builder.hll {
                        hll.update_scalar(&s.as_ref(), *num_rows as u64);
                    }
                    if let Some((top_n, top_n_size)) = &mut column_builder.top_n {
                        top_n.add_with_size(*top_n_size, s.as_ref(), *num_rows as u64);
                    }
                }
                BlockEntry::Column(col) => {
                    if col.check_large_string() {
                        keys_to_remove.push(index);
                        continue;
                    }
                    if let Some(hll) = &mut column_builder.hll {
                        hll.update_column(col);
                    }
                    if let Some((top_n, top_n_size)) = &mut column_builder.top_n {
                        for row in 0..col.len() {
                            if let Some(scalar) = col.index(row) {
                                top_n.add_with_size(*top_n_size, scalar, 1);
                            }
                        }
                    }
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
            .filter_map(|item| {
                item.hll
                    .as_ref()
                    .map(|hll| (item.field.column_id(), hll.peek()))
            })
            .collect()
    }

    pub fn finalize(self) -> Result<Option<BlockHLL>> {
        self.finalize_with_top_n()
            .map(|stats| stats.map(|(hll, _)| hll))
    }

    pub fn finalize_with_top_n(self) -> Result<Option<(BlockHLL, BlockTopN)>> {
        if self.builders.is_empty() {
            return Ok(None);
        }

        let mut column_hlls = HashMap::with_capacity(self.builders.len());
        let mut column_top_n = HashMap::with_capacity(self.builders.len());
        for column_builder in self.builders {
            let column_id = column_builder.field.column_id();
            if let Some(hll) = column_builder.hll {
                column_hlls.insert(column_id, hll.into_hll());
            }
            if let Some((top_n, top_n_size)) = column_builder.top_n
                && !top_n.values.is_empty()
            {
                column_top_n.insert(column_id, top_n.finish_with_size(top_n_size));
            }
        }

        if column_hlls.is_empty() && column_top_n.is_empty() {
            Ok(None)
        } else {
            Ok(Some((column_hlls, column_top_n)))
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::Int32Type;

    use super::*;

    fn int32_ndv_columns_map() -> BTreeMap<FieldIndex, TableField> {
        let mut ndv_columns_map = BTreeMap::new();
        ndv_columns_map.insert(
            0,
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
        );
        ndv_columns_map
    }

    fn empty_columns_map() -> BTreeMap<FieldIndex, TableField> {
        BTreeMap::new()
    }

    fn int32_block() -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 1, 2, 3])])
    }

    #[test]
    fn test_block_stats_builder_without_top_n() -> Result<()> {
        let ndv_columns_map = int32_ndv_columns_map();
        let mut builder = BlockStatsBuilder::new(&ndv_columns_map, None);
        builder.add_block(&int32_block())?;

        let (_, top_n) = builder.finalize_with_top_n()?.unwrap();
        assert!(top_n.is_empty());
        Ok(())
    }

    #[test]
    fn test_block_stats_builder_with_top_n() -> Result<()> {
        let ndv_columns_map = int32_ndv_columns_map();
        let mut builder = BlockStatsBuilder::new(&ndv_columns_map, Some((&ndv_columns_map, 2)));
        builder.add_block(&int32_block())?;

        let (_, top_n) = builder.finalize_with_top_n()?.unwrap();
        assert_eq!(top_n.len(), 1);
        assert!(top_n.values().all(|column_top_n| {
            !column_top_n.values.is_empty() && column_top_n.values.len() <= 2
        }));
        Ok(())
    }

    #[test]
    fn test_block_stats_builder_with_top_n_only() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let top_n_columns_map = int32_ndv_columns_map();
        let mut builder = BlockStatsBuilder::new(&ndv_columns_map, Some((&top_n_columns_map, 2)));
        builder.add_block(&int32_block())?;

        let (hll, top_n) = builder.finalize_with_top_n()?.unwrap();
        assert!(hll.is_empty());
        assert_eq!(top_n.len(), 1);
        Ok(())
    }
}
