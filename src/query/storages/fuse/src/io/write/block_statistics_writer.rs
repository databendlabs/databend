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
use databend_common_expression::LARGE_STRING_BYTES_THRESHOLD;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableField;
use databend_storages_common_table_meta::meta::BlockCountMinSketch;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ColumnCountMinSketch;
use databend_storages_common_table_meta::meta::ColumnTopN;

use crate::io::write::stream::ColumnNDVEstimator;
use crate::io::write::stream::ColumnNDVEstimatorOps;
use crate::io::write::stream::create_column_ndv_estimator;

pub fn build_column_hlls(
    block: &DataBlock,
    ndv_columns_map: &BTreeMap<FieldIndex, TableField>,
) -> Result<Option<BlockHLL>> {
    let mut builder = BlockStatsBuilder::new(ndv_columns_map, None, None)?;
    builder.add_block(block)?;
    builder.finalize()
}

pub struct BlockStatsBuilder {
    builders: Vec<ColumnNDVBuilder>,
}

pub struct BlockStats {
    pub hll: BlockHLL,
    pub top_n: BlockTopN,
    pub count_min_sketch: BlockCountMinSketch,
    pub dropped_top_n: Vec<ColumnId>,
}

pub struct ColumnNDVBuilder {
    index: FieldIndex,
    field: TableField,
    pub hll: Option<ColumnNDVEstimator>,
    pub top_n: Option<ColumnTopN>,
    pub count_min_sketch: Option<ColumnCountMinSketch>,
}

impl BlockStatsBuilder {
    pub fn new(
        ndv_columns_map: &BTreeMap<FieldIndex, TableField>,
        top_n: Option<(&BTreeMap<FieldIndex, TableField>, usize)>,
        count_min_sketch: Option<(&BTreeMap<FieldIndex, TableField>, f64)>,
    ) -> Result<BlockStatsBuilder> {
        let top_n_columns_map = top_n.map(|(columns, _)| columns);
        let top_n_size = top_n.map(|(_, size)| size);
        let count_min_sketch_columns_map = count_min_sketch.map(|(columns, _)| columns);
        let count_min_sketch_error_rate = count_min_sketch.map(|(_, error_rate)| error_rate);
        let mut builders = Vec::with_capacity(
            ndv_columns_map.len()
                + top_n_columns_map.map_or(0, |v| v.len())
                + count_min_sketch_columns_map.map_or(0, |v| v.len()),
        );

        for (index, field) in ndv_columns_map {
            let column_top_n = top_n_columns_map
                .zip(top_n_size)
                .and_then(|(columns, size)| columns.contains_key(index).then_some(size))
                .map(ColumnTopN::with_capacity);
            let count_min_sketch = count_min_sketch_columns_map
                .zip(count_min_sketch_error_rate)
                .filter(|(columns, _)| columns.contains_key(index))
                .map(|(_, error_rate)| ColumnCountMinSketch::with_error_rate(error_rate))
                .transpose()?;
            builders.push(ColumnNDVBuilder {
                index: *index,
                field: field.clone(),
                hll: Some(create_column_ndv_estimator(&field.data_type().into())),
                top_n: column_top_n,
                count_min_sketch,
            });
        }

        let mut frequency_columns_map = BTreeMap::new();
        if let Some(top_n_columns_map) = top_n_columns_map {
            frequency_columns_map.extend(top_n_columns_map.clone());
        }
        if let Some(count_min_sketch_columns_map) = count_min_sketch_columns_map {
            frequency_columns_map.extend(count_min_sketch_columns_map.clone());
        }
        for (index, field) in frequency_columns_map {
            if ndv_columns_map.contains_key(&index) {
                continue;
            }
            let column_top_n = top_n_columns_map
                .zip(top_n_size)
                .and_then(|(columns, size)| columns.contains_key(&index).then_some(size))
                .map(ColumnTopN::with_capacity);
            let count_min_sketch = count_min_sketch_columns_map
                .zip(count_min_sketch_error_rate)
                .filter(|(columns, _)| columns.contains_key(&index))
                .map(|(_, error_rate)| ColumnCountMinSketch::with_error_rate(error_rate))
                .transpose()?;
            if column_top_n.is_some() || count_min_sketch.is_some() {
                builders.push(ColumnNDVBuilder {
                    index,
                    field,
                    hll: None,
                    top_n: column_top_n,
                    count_min_sketch,
                });
            }
        }

        Ok(BlockStatsBuilder { builders })
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        for column_builder in self.builders.iter_mut() {
            let entry = block.get_by_offset(column_builder.index);
            match entry {
                BlockEntry::Const(s, _, num_rows) => {
                    if let Some(hll) = &mut column_builder.hll {
                        hll.update_scalar(&s.as_ref(), *num_rows as u64);
                    }
                    if should_collect_top_n_scalar(&s.as_ref())
                        && let Some(top_n) = &mut column_builder.top_n
                    {
                        top_n.add(s.as_ref(), *num_rows as u64);
                    }
                    if let Some(count_min_sketch) = &mut column_builder.count_min_sketch {
                        count_min_sketch.add_with_count(s.as_ref(), *num_rows as u64);
                    }
                }
                BlockEntry::Column(col) => {
                    if col.check_large_string() {
                        column_builder.hll = None;
                    } else if let Some(hll) = &mut column_builder.hll {
                        hll.update_column(col);
                    }
                    if column_builder.top_n.is_some() || column_builder.count_min_sketch.is_some() {
                        let mut row = 0;
                        while row < col.len() {
                            if let Some(scalar) = col.index(row) {
                                let mut count = 1;
                                while row + count < col.len()
                                    && col.index(row + count).is_some_and(|next| next == scalar)
                                {
                                    count += 1;
                                }
                                if should_collect_top_n_scalar(&scalar)
                                    && let Some(top_n) = &mut column_builder.top_n
                                {
                                    top_n.add(scalar.clone(), count as u64);
                                }
                                if let Some(count_min_sketch) = &mut column_builder.count_min_sketch
                                {
                                    count_min_sketch.add_with_count(scalar, count as u64);
                                }
                                row += count;
                            } else {
                                row += 1;
                            }
                        }
                    }
                }
            }
        }

        self.builders.retain(|builder| {
            builder.hll.is_some() || builder.top_n.is_some() || builder.count_min_sketch.is_some()
        });
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
            .map(|stats| stats.map(|stats| stats.hll))
    }

    pub fn finalize_with_top_n(self) -> Result<Option<BlockStats>> {
        let BlockStatsBuilder { builders } = self;
        if builders.is_empty() {
            return Ok(None);
        }

        let mut column_hlls = HashMap::with_capacity(builders.len());
        let mut column_top_n = HashMap::with_capacity(builders.len());
        let mut column_count_min_sketch = HashMap::with_capacity(builders.len());
        for column_builder in builders {
            let column_id = column_builder.field.column_id();
            if let Some(hll) = column_builder.hll {
                column_hlls.insert(column_id, hll.into_hll());
            }
            if let Some(top_n) = column_builder.top_n
                && !top_n.values.is_empty()
            {
                column_top_n.insert(column_id, top_n.finish());
            }
            if let Some(count_min_sketch) = column_builder.count_min_sketch
                && !count_min_sketch.is_empty()
            {
                column_count_min_sketch.insert(column_id, count_min_sketch);
            }
        }

        if column_hlls.is_empty() && column_top_n.is_empty() && column_count_min_sketch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(BlockStats {
                hll: column_hlls,
                top_n: column_top_n,
                count_min_sketch: column_count_min_sketch,
                dropped_top_n: vec![],
            }))
        }
    }
}

fn should_collect_top_n_scalar(scalar: &ScalarRef<'_>) -> bool {
    match scalar {
        ScalarRef::String(value) => value.len() <= LARGE_STRING_BYTES_THRESHOLD,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_expression::types::string::StringType;

    use super::*;

    fn int32_ndv_columns_map() -> BTreeMap<FieldIndex, TableField> {
        let mut ndv_columns_map = BTreeMap::new();
        ndv_columns_map.insert(
            0,
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
        );
        ndv_columns_map
    }

    fn string_columns_map() -> BTreeMap<FieldIndex, TableField> {
        let mut columns_map = BTreeMap::new();
        columns_map.insert(0, TableField::new("s", TableDataType::String));
        columns_map
    }

    fn empty_columns_map() -> BTreeMap<FieldIndex, TableField> {
        BTreeMap::new()
    }

    fn int32_block() -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 1, 2, 3])])
    }

    fn int32_block_with_delayed_hot_run() -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2, 2, 2])])
    }

    fn small_string_block() -> DataBlock {
        DataBlock::new_from_columns(vec![StringType::from_data(vec!["hot", "hot", "cold"])])
    }

    fn large_string_block() -> DataBlock {
        let value = "x".repeat(LARGE_STRING_BYTES_THRESHOLD + 1);
        DataBlock::new_from_columns(vec![StringType::from_data(vec![value])])
    }

    #[test]
    fn test_block_stats_builder_without_top_n() -> Result<()> {
        let ndv_columns_map = int32_ndv_columns_map();
        let mut builder = BlockStatsBuilder::new(&ndv_columns_map, None, None)?;
        builder.add_block(&int32_block())?;

        let stats = builder.finalize_with_top_n()?.unwrap();
        let top_n = stats.top_n;
        assert!(top_n.is_empty());
        assert!(stats.count_min_sketch.is_empty());
        Ok(())
    }

    #[test]
    fn test_block_stats_builder_with_top_n() -> Result<()> {
        let ndv_columns_map = int32_ndv_columns_map();
        let mut builder =
            BlockStatsBuilder::new(&ndv_columns_map, Some((&ndv_columns_map, 2)), None)?;
        builder.add_block(&int32_block())?;

        let stats = builder.finalize_with_top_n()?.unwrap();
        let top_n = stats.top_n;
        assert_eq!(top_n.len(), 1);
        assert!(top_n.values().all(|column_top_n| {
            !column_top_n.values.is_empty() && column_top_n.values.len() <= 2
        }));
        assert!(stats.count_min_sketch.is_empty());
        Ok(())
    }

    #[test]
    fn test_block_stats_builder_with_top_n_only() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let top_n_columns_map = int32_ndv_columns_map();
        let mut builder =
            BlockStatsBuilder::new(&ndv_columns_map, Some((&top_n_columns_map, 2)), None)?;
        builder.add_block(&int32_block())?;

        let stats = builder.finalize_with_top_n()?.unwrap();
        let hll = stats.hll;
        let top_n = stats.top_n;
        assert!(hll.is_empty());
        assert_eq!(top_n.len(), 1);
        assert!(stats.count_min_sketch.is_empty());
        Ok(())
    }

    #[test]
    fn test_block_stats_builder_with_count_min_sketch_only() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let count_min_sketch_columns_map = int32_ndv_columns_map();
        let mut builder = BlockStatsBuilder::new(
            &ndv_columns_map,
            None,
            Some((&count_min_sketch_columns_map, 0.01)),
        )?;
        builder.add_block(&int32_block_with_delayed_hot_run())?;

        let stats = builder.finalize_with_top_n()?.unwrap();
        assert!(stats.hll.is_empty());
        assert!(stats.top_n.is_empty());
        let count_min_sketch = stats.count_min_sketch.values().next().unwrap();
        assert_eq!(
            count_min_sketch.estimate(&Scalar::Number(NumberScalar::Int32(2))),
            Some(3)
        );
        Ok(())
    }

    #[test]
    fn test_block_stats_builder_counts_top_n_runs_before_pruning() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let top_n_columns_map = int32_ndv_columns_map();
        let mut builder = BlockStatsBuilder::new(
            &ndv_columns_map,
            Some((&top_n_columns_map, 1)),
            Some((&top_n_columns_map, 0.01)),
        )?;
        builder.add_block(&int32_block_with_delayed_hot_run())?;

        let stats = builder.finalize_with_top_n()?.unwrap();
        let top_n = stats.top_n;
        let column_top_n = top_n.values().next().unwrap();
        assert_eq!(column_top_n.values.len(), 1);
        let entry = &column_top_n.values[0];
        assert_eq!(entry.scalar, Scalar::Number(NumberScalar::Int32(2)));
        assert_eq!(entry.count, 4);
        assert_eq!(entry.error, 1);
        let count_min_sketch = stats.count_min_sketch.values().next().unwrap();
        assert_eq!(
            count_min_sketch.estimate(&Scalar::Number(NumberScalar::Int32(2))),
            Some(3)
        );
        Ok(())
    }

    #[test]
    fn test_block_stats_builder_skips_large_string_top_n_value() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let top_n_columns_map = string_columns_map();
        let column_id = top_n_columns_map.get(&0).unwrap().column_id();
        let mut builder = BlockStatsBuilder::new(
            &ndv_columns_map,
            Some((&top_n_columns_map, 2)),
            Some((&top_n_columns_map, 0.01)),
        )?;
        builder.add_block(&small_string_block())?;
        builder.add_block(&large_string_block())?;

        let stats = builder.finalize_with_top_n()?.unwrap();
        let column_top_n = stats.top_n.values().next().unwrap();
        assert_eq!(column_top_n.values.len(), 2);
        assert!(
            column_top_n.values.iter().all(|entry| {
                matches!(&entry.scalar, Scalar::String(value) if value.len() <= LARGE_STRING_BYTES_THRESHOLD)
            })
        );
        let count_min_sketch = stats.count_min_sketch.get(&column_id).unwrap();
        assert!(
            count_min_sketch
                .estimate(&Scalar::String("x".repeat(257)))
                .is_some_and(|count| count >= 1)
        );
        assert!(stats.dropped_top_n.is_empty());
        Ok(())
    }

    #[test]
    fn test_block_stats_builder_keeps_cms_for_large_string_without_top_n() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let count_min_sketch_columns_map = string_columns_map();
        let column_id = count_min_sketch_columns_map.get(&0).unwrap().column_id();
        let mut builder = BlockStatsBuilder::new(
            &ndv_columns_map,
            None,
            Some((&count_min_sketch_columns_map, 0.01)),
        )?;
        builder.add_block(&large_string_block())?;

        let stats = builder.finalize_with_top_n()?.unwrap();
        assert!(stats.hll.is_empty());
        assert!(stats.top_n.is_empty());
        assert!(stats.dropped_top_n.is_empty());
        let count_min_sketch = stats.count_min_sketch.get(&column_id).unwrap();
        assert!(
            count_min_sketch
                .estimate(&Scalar::String("x".repeat(257)))
                .is_some_and(|count| count >= 1)
        );
        Ok(())
    }

    #[test]
    fn test_block_stats_builder_skips_hll_and_keeps_cms_for_large_string_without_top_n()
    -> Result<()> {
        let ndv_columns_map = string_columns_map();
        let column_id = ndv_columns_map.get(&0).unwrap().column_id();
        let mut builder =
            BlockStatsBuilder::new(&ndv_columns_map, None, Some((&ndv_columns_map, 0.01)))?;
        builder.add_block(&large_string_block())?;

        let stats = builder.finalize_with_top_n()?.unwrap();
        assert!(stats.hll.is_empty());
        assert!(stats.top_n.is_empty());
        assert!(stats.dropped_top_n.is_empty());
        assert!(stats.count_min_sketch.contains_key(&column_id));
        Ok(())
    }
}
