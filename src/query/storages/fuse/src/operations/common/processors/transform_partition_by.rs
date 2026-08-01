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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NumberColumn;
use databend_common_pipeline_transforms::AccumulatingTransform;

use crate::operations::mutation::SerializeDataMeta;

/// Splits a block that is already sorted by the physical Fuse key into blocks
/// whose partition-key prefix is constant.
///
/// This is a per-block boundary invariant; fragments of the same partition from
/// different input blocks are not merged here.
pub struct TransformPartitionBy {
    partition_key_indices: Arc<[usize]>,
    rewrite_replaced_block: bool,
}

impl TransformPartitionBy {
    pub fn new(partition_key_indices: Arc<[usize]>) -> Self {
        Self {
            partition_key_indices,
            rewrite_replaced_block: false,
        }
    }

    pub fn new_for_update(partition_key_indices: Arc<[usize]>) -> Self {
        Self {
            partition_key_indices,
            // A direct UPDATE normally replaces one old block with one new block. Partition
            // splitting makes that one-to-many, so rewrite it as one delete plus appends.
            rewrite_replaced_block: true,
        }
    }
}

fn partition_boundaries(block: &DataBlock, indices: &[usize]) -> Vec<usize> {
    let rows = block.num_rows();
    if rows <= 1 || indices.is_empty() {
        return Vec::new();
    }

    let mut changed = vec![false; rows];
    for index in indices {
        mark_column_boundaries(block.get_by_offset(*index), &mut changed);
    }

    changed
        .into_iter()
        .enumerate()
        .skip(1)
        .filter_map(|(row, changed)| changed.then_some(row))
        .collect()
}

fn mark_column_boundaries(entry: &BlockEntry, changed: &mut [bool]) {
    if let BlockEntry::Column(column) = entry {
        mark_column_boundaries_inner(column, changed);
    }
}

fn mark_column_boundaries_inner(column: &Column, changed: &mut [bool]) {
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
        Column::Number(column) => mark_number_boundaries(column, changed),
        Column::Decimal(column) => mark_decimal_boundaries(column, changed),
        Column::Boolean(column) => mark_iter_boundaries(column.iter(), changed),
        Column::String(column) => mark_iter_boundaries(column.iter(), changed),
        Column::Timestamp(column) => mark_iter_boundaries(column.iter(), changed),
        Column::TimestampTz(column) => mark_iter_boundaries(column.iter(), changed),
        Column::Date(column) => mark_iter_boundaries(column.iter(), changed),
        Column::Nullable(column) => mark_nullable_boundaries(column, changed),
        _ => mark_iter_boundaries(column.iter(), changed),
    }
}

fn mark_number_boundaries(column: &NumberColumn, changed: &mut [bool]) {
    match column {
        NumberColumn::UInt8(column) => mark_iter_boundaries(column.iter(), changed),
        NumberColumn::UInt16(column) => mark_iter_boundaries(column.iter(), changed),
        NumberColumn::UInt32(column) => mark_iter_boundaries(column.iter(), changed),
        NumberColumn::UInt64(column) => mark_iter_boundaries(column.iter(), changed),
        NumberColumn::Int8(column) => mark_iter_boundaries(column.iter(), changed),
        NumberColumn::Int16(column) => mark_iter_boundaries(column.iter(), changed),
        NumberColumn::Int32(column) => mark_iter_boundaries(column.iter(), changed),
        NumberColumn::Int64(column) => mark_iter_boundaries(column.iter(), changed),
        NumberColumn::Float32(column) => mark_iter_boundaries(column.iter(), changed),
        NumberColumn::Float64(column) => mark_iter_boundaries(column.iter(), changed),
    }
}

fn mark_decimal_boundaries(column: &DecimalColumn, changed: &mut [bool]) {
    match column {
        DecimalColumn::Decimal64(column, _) => mark_iter_boundaries(column.iter(), changed),
        DecimalColumn::Decimal128(column, _) => mark_iter_boundaries(column.iter(), changed),
        DecimalColumn::Decimal256(column, _) => mark_iter_boundaries(column.iter(), changed),
    }
}

fn mark_nullable_boundaries(column: &NullableColumn<AnyType>, changed: &mut [bool]) {
    let mut value_changed = vec![false; changed.len()];
    mark_column_boundaries_inner(&column.column, &mut value_changed);

    let mut validity = column.validity.iter();
    let Some(mut previous_valid) = validity.next() else {
        return;
    };
    for (offset, valid) in validity.enumerate() {
        let row = offset + 1;
        changed[row] |= valid != previous_valid || valid && value_changed[row];
        previous_valid = valid;
    }
}

fn mark_iter_boundaries<T: PartialEq>(values: impl Iterator<Item = T>, changed: &mut [bool]) {
    let mut values = values;
    let Some(mut previous) = values.next() else {
        return;
    };
    for (offset, value) in values.enumerate() {
        let row = offset + 1;
        changed[row] |= value != previous;
        previous = value;
    }
}

impl AccumulatingTransform for TransformPartitionBy {
    const NAME: &'static str = "TransformPartitionBy";

    fn transform(&mut self, mut block: DataBlock) -> Result<Vec<DataBlock>> {
        if self.partition_key_indices.is_empty()
            || block.num_rows() == 0
            || block.num_rows() == 1 && !self.rewrite_replaced_block
        {
            return Ok(vec![block]);
        }

        let replaced_block_meta = if self.rewrite_replaced_block {
            block.take_meta()
        } else {
            None
        };
        let boundaries = partition_boundaries(&block, &self.partition_key_indices);
        let mut blocks = Vec::with_capacity(boundaries.len() + 1);
        let mut start = 0;
        for end in boundaries {
            blocks.push(block.slice(start..end));
            start = end;
        }
        blocks.push(block.slice(start..block.num_rows()));

        if let Some(meta) = replaced_block_meta {
            let meta = SerializeDataMeta::downcast_from(meta)
                .ok_or_else(|| ErrorCode::Internal("Invalid partitioned UPDATE block metadata"))?;
            let SerializeDataMeta::SerializeBlock(serialize_block) = meta else {
                return Err(ErrorCode::Internal(
                    "Invalid partitioned UPDATE block metadata",
                ));
            };
            for block in &mut blocks {
                block.replace_meta(Box::new(SerializeDataMeta::SerializeAppend));
            }
            blocks.insert(
                0,
                DataBlock::empty_with_meta(Box::new(SerializeDataMeta::SerializeBlock(
                    serialize_block,
                ))),
            );
        }
        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::StringType;

    use super::*;
    use crate::operations::common::BlockMetaIndex;
    use crate::operations::mutation::ClusterStatsGenType;
    use crate::operations::mutation::SerializeBlock;

    #[test]
    fn test_partition_boundaries() {
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![0, 0, 1, 1, 1]),
            StringType::from_data(vec!["a", "a", "a", "b", "b"]),
        ]);
        assert_eq!(partition_boundaries(&block, &[0, 1]), vec![2, 3]);

        let block = DataBlock::new_from_columns(vec![Int32Type::from_opt_data(vec![
            None,
            None,
            Some(1),
            Some(1),
            Some(2),
        ])]);
        assert_eq!(partition_boundaries(&block, &[0]), vec![2, 4]);
    }

    #[test]
    fn test_split_sorted_block_by_partition_prefix() -> Result<()> {
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![0, 0, 1, 1, 2]),
            Int32Type::from_data(vec![1, 2, 1, 2, 1]),
        ]);
        let mut transform = TransformPartitionBy::new(Arc::from([0]));
        let blocks = transform.transform(block)?;

        assert_eq!(blocks.len(), 3);
        assert_eq!(
            blocks.iter().map(DataBlock::num_rows).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
        Ok(())
    }

    #[test]
    fn test_split_replaced_block_as_delete_and_append() -> Result<()> {
        let meta = SerializeDataMeta::SerializeBlock(SerializeBlock::create(
            BlockMetaIndex::default(),
            ClusterStatsGenType::Generally,
            2,
            0,
            None,
        ));
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![0, 0, 1, 1, 2]),
            Int32Type::from_data(vec![1, 2, 1, 2, 1]),
        ])
        .add_meta(Some(Box::new(meta)))?;
        let mut transform = TransformPartitionBy::new_for_update(Arc::from([0]));
        let mut blocks = transform.transform(block)?;

        assert_eq!(blocks.len(), 4);
        assert!(blocks[0].is_empty());
        assert!(matches!(
            SerializeDataMeta::downcast_from(blocks[0].take_meta().unwrap()),
            Some(SerializeDataMeta::SerializeBlock(SerializeBlock {
                logical_updated_rows: 2,
                logical_deleted_rows: 0,
                ..
            }))
        ));
        assert!(blocks[1..].iter_mut().all(|block| matches!(
            SerializeDataMeta::downcast_from(block.take_meta().unwrap()),
            Some(SerializeDataMeta::SerializeAppend)
        )));
        assert_eq!(
            blocks[1..]
                .iter()
                .map(DataBlock::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );

        let meta = SerializeDataMeta::SerializeBlock(SerializeBlock::create(
            BlockMetaIndex::default(),
            ClusterStatsGenType::Generally,
            1,
            0,
            None,
        ));
        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1])])
            .add_meta(Some(Box::new(meta)))?;
        let mut transform = TransformPartitionBy::new_for_update(Arc::from([0]));
        let mut blocks = transform.transform(block)?;
        assert_eq!(blocks.len(), 2);
        assert!(blocks[0].is_empty());
        assert_eq!(blocks[1].num_rows(), 1);
        assert!(matches!(
            SerializeDataMeta::downcast_from(blocks[1].take_meta().unwrap()),
            Some(SerializeDataMeta::SerializeAppend)
        ));
        Ok(())
    }
}
