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

use std::collections::HashSet;

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::Index;

use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::types::AnyType;
use crate::types::ArrayColumn;
use crate::types::BinaryColumn;
use crate::types::DecimalColumn;
use crate::types::DecimalColumnBuilder;
use crate::types::NullableColumn;
use crate::types::NumberColumn;
use crate::types::NumberColumnBuilder;
use crate::types::OpaqueColumn;
use crate::types::OpaqueColumnBuilder;
use crate::types::StringColumn;
use crate::types::VectorColumn;
use crate::types::VectorColumnBuilder;
use crate::types::array::ArrayColumnBuilder;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::nullable::NullableColumnBuilder;
use crate::types::string::StringColumnBuilder;
use crate::with_decimal_type;
use crate::with_number_mapped_type;

struct PartitionBlockBuilder {
    num_rows: usize,
    columns_builder: Vec<ColumnBuilder>,
}

pub struct BlockPartitionStream {
    initialize: bool,
    scatter_size: usize,
    rows_threshold: usize,
    bytes_threshold: usize,
    partitions: Vec<PartitionBlockBuilder>,
}

impl BlockPartitionStream {
    pub fn create(
        mut rows_threshold: usize,
        mut bytes_threshold: usize,
        scatter_size: usize,
    ) -> BlockPartitionStream {
        if rows_threshold == 0 {
            rows_threshold = usize::MAX;
        }

        if bytes_threshold == 0 {
            bytes_threshold = usize::MAX;
        }

        BlockPartitionStream {
            scatter_size,
            rows_threshold,
            bytes_threshold,
            initialize: false,
            partitions: vec![],
        }
    }

    pub fn partition(
        &mut self,
        indices: Vec<u64>,
        block: DataBlock,
        out_ready: bool,
    ) -> Vec<(usize, DataBlock)> {
        if block.is_empty() {
            return vec![];
        }

        if !self.initialize {
            self.initialize = true;

            self.partitions.reserve(self.scatter_size);
            for _ in 0..self.scatter_size {
                let mut columns_builder = Vec::with_capacity(block.num_columns());

                for column in block.columns() {
                    let data_type = column.data_type();
                    columns_builder.push(ColumnBuilder::with_capacity(&data_type, 0));
                }

                let block_builder = PartitionBlockBuilder {
                    num_rows: 0,
                    columns_builder,
                };
                self.partitions.push(block_builder);
            }
        }

        let columns = block
            .take_columns()
            .into_iter()
            .map(|x| x.to_column())
            .collect::<Vec<_>>();

        let scatter_indices =
            DataBlock::divide_indices_by_scatter_size(&indices, self.scatter_size);

        for (partition_id, indices) in scatter_indices.iter().enumerate() {
            self.partitions[partition_id].num_rows += indices.len();
        }

        for (column_idx, column) in columns.into_iter().enumerate() {
            for (partition_id, indices) in scatter_indices.iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }

                let partition = &mut self.partitions[partition_id];
                let column_builder = &mut partition.columns_builder[column_idx];
                copy_column(indices, &column, column_builder);
            }

            drop(column);
        }

        if !out_ready {
            return vec![];
        }

        let mut ready_blocks = Vec::with_capacity(self.partitions.len());
        for (id, partition) in self.partitions.iter_mut().enumerate() {
            let memory_size = partition
                .columns_builder
                .iter()
                .map(|x| x.memory_size())
                .sum::<usize>();

            let rows = partition.num_rows;

            if memory_size >= self.bytes_threshold || rows >= self.rows_threshold {
                let mut columns = Vec::with_capacity(partition.columns_builder.len());
                let columns_builder = std::mem::take(&mut partition.columns_builder);
                partition.columns_builder.reserve(columns_builder.len());

                for column_builder in columns_builder {
                    let historical_size = column_builder.len();
                    let data_type = column_builder.data_type();
                    let new_builder = ColumnBuilder::with_capacity(&data_type, historical_size);
                    partition.columns_builder.push(new_builder);
                    columns.push(BlockEntry::from(column_builder.build()));
                }

                partition.num_rows = 0;
                let block = DataBlock::new(columns, rows);
                for sub_block in split_block_if_needed(block, self.rows_threshold) {
                    ready_blocks.push((id, sub_block));
                }
            }
        }

        ready_blocks
    }

    pub fn partition_ids(&self) -> Vec<usize> {
        let mut partition_ids = vec![];

        if !self.initialize {
            return partition_ids;
        }

        for (partition_id, data) in self.partitions.iter().enumerate() {
            if data.num_rows != 0 {
                partition_ids.push(partition_id);
            }
        }
        partition_ids
    }

    pub fn take_partitions(&mut self, excluded: &HashSet<usize>) -> Vec<(usize, DataBlock)> {
        if !self.initialize {
            return vec![];
        }

        let capacity = self.partitions.len() - excluded.len();

        let mut take_blocks = Vec::with_capacity(capacity);

        for (id, partition) in self.partitions.iter_mut().enumerate() {
            if excluded.contains(&id) {
                continue;
            }

            let mut columns = Vec::with_capacity(partition.columns_builder.len());
            let columns_builder = std::mem::take(&mut partition.columns_builder);
            partition.columns_builder.reserve(columns_builder.len());

            for column_builder in columns_builder {
                let historical_size = column_builder.len();
                let data_type = column_builder.data_type();
                let new_builder = ColumnBuilder::with_capacity(&data_type, historical_size);
                partition.columns_builder.push(new_builder);
                columns.push(BlockEntry::from(column_builder.build()));
            }

            let num_rows = partition.num_rows;
            partition.num_rows = 0;
            let block = DataBlock::new(columns, num_rows);
            for sub_block in split_block_if_needed(block, self.rows_threshold) {
                take_blocks.push((id, sub_block));
            }
        }

        take_blocks
    }

    pub fn finalize_partition(&mut self, partition_id: usize) -> Vec<DataBlock> {
        if !self.initialize {
            return vec![];
        }

        let partition = &mut self.partitions[partition_id];

        let num_rows = partition.num_rows;

        if num_rows == 0 {
            return vec![];
        }

        let mut columns = Vec::with_capacity(partition.columns_builder.len());
        let columns_builder = std::mem::take(&mut partition.columns_builder);
        partition.columns_builder.reserve(columns_builder.len());

        for column_builder in columns_builder {
            let data_type = column_builder.data_type();
            let new_builder = ColumnBuilder::with_capacity(&data_type, 0);
            partition.columns_builder.push(new_builder);
            columns.push(BlockEntry::from(column_builder.build()));
        }

        partition.num_rows = 0;
        let block = DataBlock::new(columns, num_rows);
        self.split_block_if_needed(block)
    }

    fn split_block_if_needed(&self, block: DataBlock) -> Vec<DataBlock> {
        split_block_if_needed(block, self.rows_threshold)
    }
}

fn split_block_if_needed(block: DataBlock, rows_threshold: usize) -> Vec<DataBlock> {
    if rows_threshold < usize::MAX && block.num_rows() > rows_threshold {
        block.split_by_rows_no_tail(rows_threshold)
    } else {
        vec![block]
    }
}

pub fn copy_column<I: Index>(indices: &[I], from: &Column, to: &mut ColumnBuilder) {
    match to {
        ColumnBuilder::EmptyArray { len } => match from {
            Column::EmptyArray { .. } => *len += indices.len(),
            Column::Array(column) => {
                let capacity = *len + column.len();
                match ColumnBuilder::with_capacity(&from.data_type(), capacity) {
                    ColumnBuilder::Array(mut builder) => {
                        builder.offsets.extend(vec![0; *len]);
                        copy_array(&mut builder, column, indices);
                        *to = ColumnBuilder::Array(builder);
                    }
                    _ => unreachable!(
                        "ColumnBuilder::with_capacity for Array type should return ColumnBuilder::Array, \
                     but got different variant. data_type: {}, capacity: {}",
                        from.data_type(),
                        capacity
                    ),
                }
            }
            _ => unreachable!(
                "EmptyArray builder can only copy from EmptyArray or Array, but got from type: {}",
                from.data_type()
            ),
        },
        ColumnBuilder::Array(builder) => match from {
            Column::EmptyArray { .. } => {
                for _ in 0..indices.len() {
                    builder.commit_row();
                }
            }
            Column::Array(column) => {
                copy_array(builder, column, indices);
            }
            _ => unreachable!(
                "Array builder can only copy from EmptyArray or Array, but got from type: {}",
                from.data_type()
            ),
        },
        ColumnBuilder::Null { len } => match from {
            Column::Null { .. } => *len += indices.len(),
            Column::Nullable(column) => {
                let capacity = *len + column.len();

                match ColumnBuilder::with_capacity(&from.data_type(), capacity) {
                    ColumnBuilder::Nullable(mut builder) => {
                        builder.push_repeat_null(*len);
                        copy_nullable(&mut builder, column, indices);
                        *to = ColumnBuilder::Nullable(builder);
                    }
                    _ => unreachable!(
                        "ColumnBuilder::with_capacity for Nullable type should return ColumnBuilder::Nullable, \
                     but got different variant. data_type: {}, capacity: {}",
                        from.data_type(),
                        capacity
                    ),
                }
            }
            _ => unreachable!(
                "Null builder can only copy from Null or Nullable, but got from type: {}",
                from.data_type()
            ),
        },
        ColumnBuilder::Nullable(builder) => match from {
            Column::Null { .. } => {
                builder.push_repeat_null(indices.len());
            }
            Column::Nullable(column) => {
                copy_nullable(builder, column, indices);
            }
            _ => unreachable!(
                "Nullable builder can only copy from Null or Nullable, but got from type: {}",
                from.data_type()
            ),
        },
        ColumnBuilder::EmptyMap { len } => match from {
            Column::EmptyMap { .. } => *len += indices.len(),
            Column::Map(column) => {
                let capacity = *len + indices.len();
                match ColumnBuilder::with_capacity(&from.data_type(), capacity) {
                    ColumnBuilder::Map(mut builder) => {
                        builder.offsets.extend(vec![0; *len]);
                        copy_array(&mut builder, column, indices);
                        *to = ColumnBuilder::Map(builder);
                    }
                    _ => unreachable!(
                        "ColumnBuilder::with_capacity for Map type should return ColumnBuilder::Map, \
                     but got different variant. data_type: {}, capacity: {}",
                        from.data_type(),
                        capacity
                    ),
                }
            }
            _ => unreachable!(
                "EmptyMap builder can only copy from EmptyMap or Map, but got from type: {}",
                from.data_type()
            ),
        },
        ColumnBuilder::Map(builder) => match from {
            Column::Map(column) => {
                copy_array(builder, column, indices);
            }
            Column::EmptyMap { .. } => {
                for _ in 0..indices.len() {
                    builder.commit_row();
                }
            }
            _ => unreachable!(
                "Map builder can only copy from EmptyMap or Map, but got from type: {}",
                from.data_type()
            ),
        },
        _ => match (to, from) {
            (ColumnBuilder::Number(builder), Column::Number(number_column)) => {
                with_number_mapped_type!(|NUM_TYPE| match (builder, number_column) {
                    (NumberColumnBuilder::NUM_TYPE(b), NumberColumn::NUM_TYPE(c)) => {
                        copy_primitive_type(b, c, indices);
                    }
                    _ => unreachable!(),
                })
            }
            (ColumnBuilder::Decimal(builder), Column::Decimal(column)) => {
                with_decimal_type!(|DECIMAL_TYPE| match (builder, column) {
                    (
                        DecimalColumnBuilder::DECIMAL_TYPE(builder, _),
                        DecimalColumn::DECIMAL_TYPE(column, _),
                    ) => {
                        copy_primitive_type(builder, column, indices);
                    }
                    _ => unreachable!(),
                });
            }
            (ColumnBuilder::Boolean(builder), Column::Boolean(column)) => {
                copy_boolean(builder, column, indices)
            }
            (ColumnBuilder::Date(builder), Column::Date(column)) => {
                copy_primitive_type(builder, column, indices);
            }
            (ColumnBuilder::Interval(builder), Column::Interval(column)) => {
                copy_primitive_type(builder, column, indices);
            }
            (ColumnBuilder::Timestamp(builder), Column::Timestamp(column)) => {
                copy_primitive_type(builder, column, indices);
            }
            (ColumnBuilder::Bitmap(builder), Column::Bitmap(column)) => {
                copy_binary(builder, column, indices);
            }
            (ColumnBuilder::Binary(builder), Column::Binary(column)) => {
                copy_binary(builder, column, indices);
            }
            (ColumnBuilder::Variant(builder), Column::Variant(column)) => {
                copy_binary(builder, column, indices);
            }
            (ColumnBuilder::Geometry(builder), Column::Geometry(column)) => {
                copy_binary(builder, column, indices);
            }
            (ColumnBuilder::Geography(builder), Column::Geography(column)) => {
                copy_binary(builder, &column.0, indices);
            }
            (ColumnBuilder::String(builder), Column::String(column)) => {
                copy_string(builder, column, indices);
            }
            (ColumnBuilder::Vector(builder), Column::Vector(column)) => {
                copy_vector(indices, builder, column);
            }
            (ColumnBuilder::Opaque(builder), Column::Opaque(column)) => {
                copy_opaque(indices, builder, column);
            }
            (ColumnBuilder::Tuple(builders), Column::Tuple(columns)) => {
                for (builder, column) in builders.iter_mut().zip(columns.iter()) {
                    copy_column(indices, column, builder)
                }
            }
            (to, from) => unreachable!(
                "Unsupported column builder type for copy_column. to type: {:?}, from type: {}",
                to.data_type(),
                from.data_type()
            ),
        },
    };
}

fn copy_boolean<I: Index>(to: &mut MutableBitmap, from: &Bitmap, indices: &[I]) {
    let num_rows = indices.len();

    if num_rows == 0 {
        return;
    }

    // Fast path: avoid iterating column to generate a new bitmap.
    // If this [`Bitmap`] is all true or all false and `num_rows <= bitmap.len()``,
    // we can just slice it.
    if num_rows <= from.len() && (from.null_count() == 0 || from.null_count() == from.len()) {
        to.extend_constant(num_rows, from.get_bit(0));
        return;
    }

    to.extend_from_trusted_len_iter(indices.iter().map(|index| from.get_bit(index.to_usize())));
}

fn copy_primitive_type<T: Copy, I: Index>(to: &mut Vec<T>, from: &Buffer<T>, indices: &[I]) {
    to.extend(
        indices
            .iter()
            .map(|index| unsafe { *from.get_unchecked(index.to_usize()) }),
    );
}

fn copy_binary<I: Index>(to: &mut BinaryColumnBuilder, from: &BinaryColumn, indices: &[I]) {
    let num_rows = indices.len();

    let row_bytes = from.total_bytes_len() / from.len();
    let data_capacity = row_bytes * (indices.len() * 4).div_ceil(3);
    to.reserve(num_rows, data_capacity);

    for index in indices.iter() {
        unsafe {
            to.put_slice(from.index_unchecked(index.to_usize()));
            to.commit_row();
        }
    }
}

fn copy_string<I: Index>(to: &mut StringColumnBuilder, from: &StringColumn, indices: &[I]) {
    to.data.reserve(indices.len());

    for index in indices.iter() {
        unsafe {
            to.put_and_commit(from.index_unchecked(index.to_usize()));
        }
    }
}

fn copy_nullable<I: Index>(
    to: &mut NullableColumnBuilder<AnyType>,
    from: &NullableColumn<AnyType>,
    indices: &[I],
) {
    copy_boolean(&mut to.validity, &from.validity, indices);
    copy_column(indices, &from.column, &mut to.builder)
}

fn copy_opaque<I: Index>(indices: &[I], builder: &mut OpaqueColumnBuilder, column: &OpaqueColumn) {
    match (builder, column) {
        (OpaqueColumnBuilder::Opaque1(builder), OpaqueColumn::Opaque1(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        (OpaqueColumnBuilder::Opaque2(builder), OpaqueColumn::Opaque2(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        (OpaqueColumnBuilder::Opaque3(builder), OpaqueColumn::Opaque3(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        (OpaqueColumnBuilder::Opaque4(builder), OpaqueColumn::Opaque4(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        (OpaqueColumnBuilder::Opaque5(builder), OpaqueColumn::Opaque5(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        (OpaqueColumnBuilder::Opaque6(builder), OpaqueColumn::Opaque6(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        _ => unreachable!(),
    }
}

fn copy_vector<I: Index>(indices: &[I], builder: &mut VectorColumnBuilder, column: &VectorColumn) {
    match (builder, column) {
        (VectorColumnBuilder::Int8((builder, _)), VectorColumn::Int8((column, _))) => {
            copy_primitive_type(builder, column, indices);
        }
        (VectorColumnBuilder::Float32((builder, _)), VectorColumn::Float32((column, _))) => {
            copy_primitive_type(builder, column, indices);
        }
        _ => unreachable!(),
    }
}

fn copy_array<I: Index>(
    to: &mut ArrayColumnBuilder<AnyType>,
    from: &ArrayColumn<AnyType>,
    indices: &[I],
) {
    let mut i = 0;
    while i < indices.len() {
        let start = indices[i].to_usize();
        let mut run_len = 1;

        while i + run_len < indices.len()
            && indices[i + run_len].to_usize() == start + run_len
        {
            run_len += 1;
        }

        if run_len >= 2 {
            let sliced = from.slice(start..start + run_len);
            to.append_column(&sliced);
        } else {
            unsafe { to.push(from.index_unchecked(start)) }
        }

        i += run_len;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FromData;
    use crate::types::ArgType;
    use crate::types::ArrayColumn;
    use crate::types::ArrayType;
    use crate::types::Int32Type;
    use crate::types::Int64Type;

    use databend_common_column::buffer::Buffer;

    fn make_block(values: Vec<i32>) -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(values)])
    }

    fn build_array_column(rows: Vec<Vec<i64>>) -> Column {
        let mut values = Vec::new();
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        offsets.push(0_u64);
        for row in rows {
            values.extend(row.into_iter());
            offsets.push(values.len() as u64);
        }
        let col = ArrayColumn::<Int64Type>::new(Buffer::from(values), Buffer::from(offsets));
        ArrayType::<Int64Type>::upcast_column(col)
    }

    fn extract_array_rows(block: &DataBlock, col_idx: usize) -> Vec<Vec<i64>> {
        let col = block.columns()[col_idx].to_column();
        match &col {
            Column::Array(arr) => {
                let mut result = Vec::new();
                for i in 0..arr.len() {
                    let inner = unsafe { arr.index_unchecked(i) };
                    match inner.as_number().unwrap() {
                        NumberColumn::Int64(buf) => result.push(buf.to_vec()),
                        _ => panic!("expected Int64"),
                    }
                }
                result
            }
            _ => panic!("expected Array column"),
        }
    }

    use crate::types::NumberColumn;

    fn collect_column_values(block: &DataBlock) -> Vec<i32> {
        let col = block.columns()[0].to_column();
        match col.as_number().unwrap() {
            NumberColumn::Int32(buf) => buf.to_vec(),
            _ => panic!("expected Int32 column"),
        }
    }

    #[test]
    fn test_partition_no_split_under_threshold() {
        let mut stream = BlockPartitionStream::create(100, 0, 2);
        // All indices go to partition 0
        let indices = vec![0u64; 50];
        let block = make_block((0..50).collect());
        let result = stream.partition(indices, block, true);
        // 50 rows < 100 threshold, nothing emitted
        assert!(result.is_empty());
    }

    #[test]
    fn test_partition_emit_at_threshold() {
        let mut stream = BlockPartitionStream::create(10, 0, 1);
        let indices = vec![0u64; 10];
        let block = make_block((0..10).collect());
        let result = stream.partition(indices, block, true);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 0);
        assert_eq!(result[0].1.num_rows(), 10);
    }

    #[test]
    fn test_partition_splits_large_block() {
        let mut stream = BlockPartitionStream::create(10, 0, 1);
        // Push 25 rows into partition 0
        let indices = vec![0u64; 25];
        let block = make_block((0..25).collect());
        let result = stream.partition(indices, block, true);
        // Should be split into blocks of 10, 10, 5
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1.num_rows(), 10);
        assert_eq!(result[1].1.num_rows(), 10);
        assert_eq!(result[2].1.num_rows(), 5);
        // All should have partition_id 0
        assert!(result.iter().all(|(id, _)| *id == 0));
        // Verify data integrity
        let all_values: Vec<i32> = result.iter().flat_map(|(_, b)| collect_column_values(b)).collect();
        assert_eq!(all_values, (0..25).collect::<Vec<i32>>());
    }

    #[test]
    fn test_partition_multiple_partitions_split() {
        let mut stream = BlockPartitionStream::create(5, 0, 2);
        // 8 rows to partition 0, 7 rows to partition 1
        let mut indices = vec![0u64; 8];
        indices.extend(vec![1u64; 7]);
        let block = make_block((0..15).collect());
        let result = stream.partition(indices, block, true);
        let p0: Vec<_> = result.iter().filter(|(id, _)| *id == 0).collect();
        let p1: Vec<_> = result.iter().filter(|(id, _)| *id == 1).collect();
        // partition 0: 8 rows -> split into 5 + 3
        assert_eq!(p0.len(), 2);
        assert_eq!(p0[0].1.num_rows(), 5);
        assert_eq!(p0[1].1.num_rows(), 3);
        // partition 1: 7 rows -> split into 5 + 2
        assert_eq!(p1.len(), 2);
        assert_eq!(p1[0].1.num_rows(), 5);
        assert_eq!(p1[1].1.num_rows(), 2);
    }

    #[test]
    fn test_finalize_partition_splits() {
        let mut stream = BlockPartitionStream::create(10, 0, 1);
        // Push 25 rows but don't emit (out_ready=false)
        let indices = vec![0u64; 25];
        let block = make_block((0..25).collect());
        let result = stream.partition(indices, block, false);
        assert!(result.is_empty());
        // Finalize should split
        let blocks = stream.finalize_partition(0);
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].num_rows(), 10);
        assert_eq!(blocks[1].num_rows(), 10);
        assert_eq!(blocks[2].num_rows(), 5);
    }

    #[test]
    fn test_finalize_empty_partition() {
        let mut stream = BlockPartitionStream::create(10, 0, 2);
        // Initialize by pushing some data to partition 0
        let indices = vec![0u64; 5];
        let block = make_block(vec![1, 2, 3, 4, 5]);
        stream.partition(indices, block, false);
        // Partition 1 has no data
        let blocks = stream.finalize_partition(1);
        assert!(blocks.is_empty());
    }

    #[test]
    fn test_take_partitions_splits() {
        let mut stream = BlockPartitionStream::create(5, 0, 3);
        // Push 12 rows to partition 0, 8 to partition 1, 3 to partition 2
        let mut indices = vec![0u64; 12];
        indices.extend(vec![1u64; 8]);
        indices.extend(vec![2u64; 3]);
        let block = make_block((0..23).collect());
        stream.partition(indices, block, false);

        // Take all except partition 2
        let excluded: HashSet<usize> = [2].into_iter().collect();
        let result = stream.take_partitions(&excluded);

        let p0: Vec<_> = result.iter().filter(|(id, _)| *id == 0).collect();
        let p1: Vec<_> = result.iter().filter(|(id, _)| *id == 1).collect();
        let p2: Vec<_> = result.iter().filter(|(id, _)| *id == 2).collect();
        // partition 0: 12 rows -> 5 + 5 + 2
        assert_eq!(p0.len(), 3);
        assert_eq!(p0[0].1.num_rows(), 5);
        assert_eq!(p0[1].1.num_rows(), 5);
        assert_eq!(p0[2].1.num_rows(), 2);
        // partition 1: 8 rows -> 5 + 3
        assert_eq!(p1.len(), 2);
        assert_eq!(p1[0].1.num_rows(), 5);
        assert_eq!(p1[1].1.num_rows(), 3);
        // partition 2 excluded
        assert!(p2.is_empty());
    }

    #[test]
    fn test_no_split_when_no_row_threshold() {
        // rows_threshold=0 means usize::MAX, no splitting
        let mut stream = BlockPartitionStream::create(0, 1, 1);
        let indices = vec![0u64; 100];
        let block = make_block((0..100).collect());
        // bytes_threshold=1 triggers emit, but no row splitting
        let result = stream.partition(indices, block, true);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1.num_rows(), 100);
    }

    #[test]
    fn test_copy_array_single_partition() {
        // All rows go to partition 0, indices are consecutive
        let array_col = build_array_column(vec![
            vec![1, 2],
            vec![3],
            vec![4, 5, 6],
            vec![7, 8],
        ]);
        let block = DataBlock::new_from_columns(vec![array_col]);

        let mut stream = BlockPartitionStream::create(0, 1, 1);
        let indices = vec![0u64; 4];
        let result = stream.partition(indices, block, true);

        assert_eq!(result.len(), 1);
        let rows = extract_array_rows(&result[0].1, 0);
        assert_eq!(rows, vec![vec![1, 2], vec![3], vec![4, 5, 6], vec![7, 8]]);
    }

    #[test]
    fn test_copy_array_multi_partition() {
        // Scatter rows across 2 partitions
        let array_col = build_array_column(vec![
            vec![10, 20],   // row 0 -> partition 0
            vec![30],       // row 1 -> partition 1
            vec![40, 50],   // row 2 -> partition 0
            vec![60, 70, 80], // row 3 -> partition 1
        ]);
        let block = DataBlock::new_from_columns(vec![array_col]);

        let mut stream = BlockPartitionStream::create(0, 1, 2);
        let indices = vec![0u64, 1, 0, 1];
        let result = stream.partition(indices, block, true);

        let p0: Vec<_> = result.iter().filter(|(id, _)| *id == 0).collect();
        let p1: Vec<_> = result.iter().filter(|(id, _)| *id == 1).collect();

        assert_eq!(p0.len(), 1);
        let p0_rows = extract_array_rows(&p0[0].1, 0);
        assert_eq!(p0_rows, vec![vec![10, 20], vec![40, 50]]);

        assert_eq!(p1.len(), 1);
        let p1_rows = extract_array_rows(&p1[0].1, 0);
        assert_eq!(p1_rows, vec![vec![30], vec![60, 70, 80]]);
    }

    #[test]
    fn test_copy_array_consecutive_indices() {
        // Indices [0,1,2,3] are fully consecutive — exercises the batch path
        let array_col = build_array_column(vec![
            vec![1],
            vec![2, 3],
            vec![4, 5, 6],
            vec![7],
        ]);
        let block = DataBlock::new_from_columns(vec![array_col]);

        let mut stream = BlockPartitionStream::create(0, 1, 1);
        let indices = vec![0u64; 4];
        let result = stream.partition(indices, block, true);

        assert_eq!(result.len(), 1);
        let rows = extract_array_rows(&result[0].1, 0);
        assert_eq!(rows, vec![vec![1], vec![2, 3], vec![4, 5, 6], vec![7]]);
    }

    #[test]
    fn test_copy_array_non_consecutive_indices() {
        // Indices [0, 2] are non-consecutive — exercises the single-element fallback
        let array_col = build_array_column(vec![
            vec![10],
            vec![20, 30],
            vec![40, 50],
        ]);
        let block = DataBlock::new_from_columns(vec![array_col]);

        let mut stream = BlockPartitionStream::create(0, 1, 2);
        // row 0 -> p0, row 1 -> p1, row 2 -> p0
        let indices = vec![0u64, 1, 0];
        let result = stream.partition(indices, block, true);

        let p0: Vec<_> = result.iter().filter(|(id, _)| *id == 0).collect();
        assert_eq!(p0.len(), 1);
        let p0_rows = extract_array_rows(&p0[0].1, 0);
        // p0 gets row 0 and row 2 (non-consecutive from source)
        assert_eq!(p0_rows, vec![vec![10], vec![40, 50]]);
    }

    #[test]
    fn test_copy_array_mixed_consecutive_and_single() {
        // 6 rows, partition them so one partition gets [0,1,2] consecutive + [4] single
        let array_col = build_array_column(vec![
            vec![1],       // row 0 -> p0
            vec![2, 3],    // row 1 -> p0
            vec![4],       // row 2 -> p0
            vec![5, 6],    // row 3 -> p1
            vec![7, 8, 9], // row 4 -> p0
            vec![10],      // row 5 -> p1
        ]);
        let block = DataBlock::new_from_columns(vec![array_col]);

        let mut stream = BlockPartitionStream::create(0, 1, 2);
        let indices = vec![0u64, 0, 0, 1, 0, 1];
        let result = stream.partition(indices, block, true);

        let p0: Vec<_> = result.iter().filter(|(id, _)| *id == 0).collect();
        let p1: Vec<_> = result.iter().filter(|(id, _)| *id == 1).collect();

        assert_eq!(p0.len(), 1);
        let p0_rows = extract_array_rows(&p0[0].1, 0);
        assert_eq!(p0_rows, vec![vec![1], vec![2, 3], vec![4], vec![7, 8, 9]]);

        assert_eq!(p1.len(), 1);
        let p1_rows = extract_array_rows(&p1[0].1, 0);
        assert_eq!(p1_rows, vec![vec![5, 6], vec![10]]);
    }

    #[test]
    fn test_copy_array_empty_inner_arrays() {
        // Some inner arrays are empty
        let array_col = build_array_column(vec![
            vec![],
            vec![1, 2],
            vec![],
            vec![3],
        ]);
        let block = DataBlock::new_from_columns(vec![array_col]);

        let mut stream = BlockPartitionStream::create(0, 1, 1);
        let indices = vec![0u64; 4];
        let result = stream.partition(indices, block, true);

        assert_eq!(result.len(), 1);
        let rows = extract_array_rows(&result[0].1, 0);
        assert_eq!(rows, vec![vec![], vec![1, 2], vec![], vec![3]]);
    }
}
