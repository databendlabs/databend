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
use databend_common_expression::Column;
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
    let mut builder = BlockColumnSketchesBuilder::new(ndv_columns_map, None, None)?;
    builder.add_block(block)?;
    builder.finalize()
}

pub struct BlockColumnSketchesBuilder {
    builders: Vec<ColumnSketchesBuilder>,
}

pub struct BlockColumnSketches {
    pub hll: BlockHLL,
    pub top_n: BlockTopN,
    pub count_min_sketch: BlockCountMinSketch,
    pub dropped_top_n: Vec<ColumnId>,
}

pub struct ColumnSketchesBuilder {
    index: FieldIndex,
    field: TableField,
    pub hll: Option<ColumnNDVEstimator>,
    pub top_n: Option<ColumnTopN>,
    pub count_min_sketch: Option<ColumnCountMinSketch>,
}

impl BlockColumnSketchesBuilder {
    pub fn new(
        ndv_columns_map: &BTreeMap<FieldIndex, TableField>,
        top_n: Option<(&BTreeMap<FieldIndex, TableField>, usize)>,
        count_min_sketch: Option<(&BTreeMap<FieldIndex, TableField>, f64)>,
    ) -> Result<BlockColumnSketchesBuilder> {
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
            builders.push(ColumnSketchesBuilder {
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
                builders.push(ColumnSketchesBuilder {
                    index,
                    field,
                    hll: None,
                    top_n: column_top_n,
                    count_min_sketch,
                });
            }
        }

        Ok(BlockColumnSketchesBuilder { builders })
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
                    if let Some(hll) = &mut column_builder.hll {
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

    /// Add one physical-schema column fragment for an explicitly selected field.
    pub fn add_column(&mut self, field_index: FieldIndex, column: &Column) -> Result<()> {
        let Some(builder_index) = self
            .builders
            .iter()
            .position(|builder| builder.index == field_index)
        else {
            return Ok(());
        };
        let builder = &mut self.builders[builder_index];
        if let Some(hll) = &mut builder.hll {
            hll.update_column(column);
        }
        if builder.top_n.is_some() || builder.count_min_sketch.is_some() {
            let mut row = 0;
            while row < column.len() {
                if let Some(scalar) = column.index(row) {
                    let mut count = 1;
                    while row + count < column.len()
                        && column.index(row + count).is_some_and(|next| next == scalar)
                    {
                        count += 1;
                    }
                    if should_collect_top_n_scalar(&scalar)
                        && let Some(top_n) = &mut builder.top_n
                    {
                        top_n.add(scalar.clone(), count as u64);
                    }
                    if let Some(sketch) = &mut builder.count_min_sketch {
                        sketch.add_with_count(scalar, count as u64);
                    }
                    row += count;
                } else {
                    row += 1;
                }
            }
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
        self.finalize_sketches()
            .map(|sketches| sketches.map(|sketches| sketches.hll))
    }

    pub fn finalize_with_top_n(self) -> Result<Option<BlockColumnSketches>> {
        self.finalize_sketches()
    }

    pub fn finalize_sketches(self) -> Result<Option<BlockColumnSketches>> {
        let BlockColumnSketchesBuilder { builders } = self;
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
            Ok(Some(BlockColumnSketches {
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
        let value = "x".repeat(257);
        DataBlock::new_from_columns(vec![StringType::from_data(vec![value])])
    }

    #[test]
    fn test_large_string_top_n_is_fragment_independent() -> Result<()> {
        let columns = string_columns_map();
        let long = "x".repeat(LARGE_STRING_BYTES_THRESHOLD + 1);
        let column = StringType::from_data(vec![long, "ok".to_string()]);

        let whole_block = DataBlock::new_from_columns(vec![column.clone()]);
        let mut whole =
            BlockColumnSketchesBuilder::new(&empty_columns_map(), Some((&columns, 2)), None)?;
        whole.add_block(&whole_block)?;
        let whole = whole.finalize_sketches()?.unwrap();

        let mut fragmented =
            BlockColumnSketchesBuilder::new(&empty_columns_map(), Some((&columns, 2)), None)?;
        fragmented.add_column(0, &column.slice(0..1))?;
        fragmented.add_column(0, &column.slice(1..2))?;
        let fragmented = fragmented.finalize_sketches()?.unwrap();

        assert_eq!(whole.top_n, fragmented.top_n);
        assert!(whole.dropped_top_n.is_empty());
        let values = &whole.top_n.values().next().unwrap().values;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].scalar, Scalar::String("ok".to_string()));
        Ok(())
    }

    #[test]
    fn test_fragmented_top_n_matches_block_path_with_run_splits_and_eviction() -> Result<()> {
        let columns = int32_ndv_columns_map();
        let column = Int32Type::from_data(vec![1, 2, 2, 2, 3, 1, 4, 4]);

        let whole_block = DataBlock::new_from_columns(vec![column.clone()]);
        let mut whole =
            BlockColumnSketchesBuilder::new(&empty_columns_map(), Some((&columns, 2)), None)?;
        whole.add_block(&whole_block)?;
        let whole = whole.finalize_sketches()?.unwrap();

        let mut fragmented =
            BlockColumnSketchesBuilder::new(&empty_columns_map(), Some((&columns, 2)), None)?;
        fragmented.add_column(0, &column.slice(0..2))?;
        fragmented.add_column(0, &column.slice(2..5))?;
        fragmented.add_column(0, &column.slice(5..8))?;
        let fragmented = fragmented.finalize_sketches()?.unwrap();

        assert_eq!(whole.top_n, fragmented.top_n);
        Ok(())
    }

    #[test]
    fn test_column_sketches_builder_without_top_n() -> Result<()> {
        let ndv_columns_map = int32_ndv_columns_map();
        let mut builder = BlockColumnSketchesBuilder::new(&ndv_columns_map, None, None)?;
        builder.add_block(&int32_block())?;

        let sketches = builder.finalize_sketches()?.unwrap();
        let top_n = sketches.top_n;
        assert!(top_n.is_empty());
        assert!(sketches.count_min_sketch.is_empty());
        Ok(())
    }

    #[test]
    fn test_column_sketches_builder_with_top_n() -> Result<()> {
        let ndv_columns_map = int32_ndv_columns_map();
        let mut builder =
            BlockColumnSketchesBuilder::new(&ndv_columns_map, Some((&ndv_columns_map, 2)), None)?;
        builder.add_block(&int32_block())?;

        let sketches = builder.finalize_sketches()?.unwrap();
        let top_n = sketches.top_n;
        assert_eq!(top_n.len(), 1);
        assert!(top_n.values().all(|column_top_n| {
            !column_top_n.values.is_empty() && column_top_n.values.len() <= 2
        }));
        assert!(sketches.count_min_sketch.is_empty());
        Ok(())
    }

    #[test]
    fn test_column_sketches_builder_with_top_n_only() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let top_n_columns_map = int32_ndv_columns_map();
        let mut builder =
            BlockColumnSketchesBuilder::new(&ndv_columns_map, Some((&top_n_columns_map, 2)), None)?;
        builder.add_block(&int32_block())?;

        let sketches = builder.finalize_sketches()?.unwrap();
        let hll = sketches.hll;
        let top_n = sketches.top_n;
        assert!(hll.is_empty());
        assert_eq!(top_n.len(), 1);
        assert!(sketches.count_min_sketch.is_empty());
        Ok(())
    }

    #[test]
    fn test_column_sketches_builder_with_count_min_sketch_only() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let count_min_sketch_columns_map = int32_ndv_columns_map();
        let mut builder = BlockColumnSketchesBuilder::new(
            &ndv_columns_map,
            None,
            Some((&count_min_sketch_columns_map, 0.01)),
        )?;
        builder.add_block(&int32_block_with_delayed_hot_run())?;

        let sketches = builder.finalize_sketches()?.unwrap();
        assert!(sketches.hll.is_empty());
        assert!(sketches.top_n.is_empty());
        let count_min_sketch = sketches.count_min_sketch.values().next().unwrap();
        assert_eq!(
            count_min_sketch.estimate(&Scalar::Number(NumberScalar::Int32(2))),
            Some(3)
        );
        Ok(())
    }

    #[test]
    fn test_column_sketches_builder_counts_top_n_runs_before_pruning() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let top_n_columns_map = int32_ndv_columns_map();
        let mut builder = BlockColumnSketchesBuilder::new(
            &ndv_columns_map,
            Some((&top_n_columns_map, 1)),
            Some((&top_n_columns_map, 0.01)),
        )?;
        builder.add_block(&int32_block_with_delayed_hot_run())?;

        let sketches = builder.finalize_sketches()?.unwrap();
        let top_n = sketches.top_n;
        let column_top_n = top_n.values().next().unwrap();
        assert_eq!(column_top_n.values.len(), 1);
        let entry = &column_top_n.values[0];
        assert_eq!(entry.scalar, Scalar::Number(NumberScalar::Int32(2)));
        assert_eq!(entry.count, 4);
        assert_eq!(entry.error, 1);
        let count_min_sketch = sketches.count_min_sketch.values().next().unwrap();
        assert_eq!(
            count_min_sketch.estimate(&Scalar::Number(NumberScalar::Int32(2))),
            Some(3)
        );
        Ok(())
    }

    #[test]
    fn test_column_sketches_builder_skips_large_string_top_n_value() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let top_n_columns_map = string_columns_map();
        let column_id = top_n_columns_map.get(&0).unwrap().column_id();
        let mut builder = BlockColumnSketchesBuilder::new(
            &ndv_columns_map,
            Some((&top_n_columns_map, 2)),
            Some((&top_n_columns_map, 0.01)),
        )?;
        builder.add_block(&small_string_block())?;
        builder.add_block(&large_string_block())?;

        let sketches = builder.finalize_sketches()?.unwrap();
        let column_top_n = sketches.top_n.values().next().unwrap();
        assert_eq!(column_top_n.values.len(), 2);
        assert!(column_top_n.values.iter().all(|entry| {
            matches!(&entry.scalar, Scalar::String(value) if value.len() <= LARGE_STRING_BYTES_THRESHOLD)
        }));
        let count_min_sketch = sketches.count_min_sketch.get(&column_id).unwrap();
        assert!(
            count_min_sketch
                .estimate(&Scalar::String("x".repeat(257)))
                .is_some_and(|count| count >= 1)
        );
        assert!(sketches.dropped_top_n.is_empty());
        Ok(())
    }

    #[test]
    fn test_column_sketches_builder_keeps_cms_for_large_string_without_top_n() -> Result<()> {
        let ndv_columns_map = empty_columns_map();
        let count_min_sketch_columns_map = string_columns_map();
        let column_id = count_min_sketch_columns_map.get(&0).unwrap().column_id();
        let mut builder = BlockColumnSketchesBuilder::new(
            &ndv_columns_map,
            None,
            Some((&count_min_sketch_columns_map, 0.01)),
        )?;
        builder.add_block(&large_string_block())?;

        let sketches = builder.finalize_sketches()?.unwrap();
        assert!(sketches.hll.is_empty());
        assert!(sketches.top_n.is_empty());
        assert!(sketches.dropped_top_n.is_empty());
        let count_min_sketch = sketches.count_min_sketch.get(&column_id).unwrap();
        assert!(
            count_min_sketch
                .estimate(&Scalar::String("x".repeat(257)))
                .is_some_and(|count| count >= 1)
        );
        Ok(())
    }

    #[test]
    fn test_column_sketches_builder_keeps_hll_and_cms_for_large_string_without_top_n() -> Result<()>
    {
        let ndv_columns_map = string_columns_map();
        let column_id = ndv_columns_map.get(&0).unwrap().column_id();
        let mut builder = BlockColumnSketchesBuilder::new(
            &ndv_columns_map,
            None,
            Some((&ndv_columns_map, 0.01)),
        )?;
        builder.add_block(&large_string_block())?;

        let sketches = builder.finalize_sketches()?.unwrap();
        assert_eq!(sketches.hll.len(), 1);
        assert_eq!(sketches.hll.get(&column_id).unwrap().count(), 1);
        assert!(sketches.top_n.is_empty());
        assert_eq!(sketches.count_min_sketch.len(), 1);
        assert!(sketches.dropped_top_n.is_empty());
        Ok(())
    }
}
