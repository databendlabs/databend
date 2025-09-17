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

use crate::types::array::ArrayColumnBuilder;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::nullable::NullableColumnBuilder;
use crate::types::string::StringColumnBuilder;
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
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;

struct PartitionBlockBuilder {
    columns_builder: Vec<ColumnBuilder>,
}

pub struct BlockPartitionStream {
    initialize: bool,
    scatter_size: usize,
    rows_threshold: usize,
    bytes_threshold: usize,
    partitions: Vec<PartitionBlockBuilder>,
    finalize_partitions: Vec<Option<DataBlock>>,
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
            finalize_partitions: vec![None; scatter_size],
        }
    }

    pub fn partition(&mut self, indices: Vec<u64>, block: DataBlock) -> Vec<(usize, DataBlock)> {
        if block.num_rows() == 0 || block.is_empty() {
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

                let block_builder = PartitionBlockBuilder { columns_builder };
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

        for (column_idx, column) in columns.into_iter().enumerate() {
            for (partition_id, indices) in scatter_indices.iter().enumerate() {
                let partition = &mut self.partitions[partition_id];
                let column_builder = &mut partition.columns_builder[column_idx];
                copy_column(indices, &column, column_builder);
            }

            drop(column);
        }

        let mut ready_blocks = Vec::with_capacity(self.partitions.len());
        for (id, partition) in self.partitions.iter_mut().enumerate() {
            let memory_size = partition
                .columns_builder
                .iter()
                .map(|x| x.memory_size())
                .sum::<usize>();

            let rows = partition.columns_builder[0].len();

            if memory_size >= self.bytes_threshold || rows >= self.rows_threshold {
                let mut columns = Vec::with_capacity(partition.columns_builder.len());
                let columns_builder = std::mem::take(&mut partition.columns_builder);
                partition.columns_builder.reserve(columns_builder.len());

                for column_builder in columns_builder {
                    let historical_size = column_builder.len();
                    let data_type = column_builder.data_type();
                    let new_builder = ColumnBuilder::with_capacity(&data_type, historical_size);
                    partition.columns_builder.push(new_builder);
                    columns.push(column_builder.build());
                }

                ready_blocks.push((id, DataBlock::new_from_columns(columns)));
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
            if data.columns_builder[0].len() != 0
                || self.finalize_partitions[partition_id].is_some()
            {
                partition_ids.push(partition_id);
            }
        }
        partition_ids
    }

    pub fn take_partitions(&mut self, excluded: &HashSet<usize>) -> Vec<(usize, DataBlock)> {
        let capacity = self.partitions.len() - excluded.len();

        let mut take_blocks = Vec::with_capacity(capacity);

        if !self.initialize {
            return take_blocks;
        }

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
                columns.push(column_builder.build());
            }

            take_blocks.push((id, DataBlock::new_from_columns(columns)));
        }

        take_blocks
    }

    pub fn destroy_finalize_partition(&mut self, partition_id: usize) -> Option<DataBlock> {
        self.finalize_partitions[partition_id].take()
    }

    pub fn finalize_partition(&mut self, partition_id: usize) -> Option<DataBlock> {
        if let Some(data_block) = &self.finalize_partitions[partition_id] {
            return Some(data_block.clone());
        }

        let partition = &mut self.partitions[partition_id];

        if !self.initialize || partition.columns_builder[0].len() == 0 {
            return None;
        }

        let mut columns = Vec::with_capacity(partition.columns_builder.len());
        let columns_builder = std::mem::take(&mut partition.columns_builder);
        partition.columns_builder.reserve(columns_builder.len());

        for column_builder in columns_builder {
            let data_type = column_builder.data_type();
            let new_builder = ColumnBuilder::with_capacity(&data_type, 0);
            partition.columns_builder.push(new_builder);
            columns.push(column_builder.build());
        }

        self.finalize_partitions[partition_id] = Some(DataBlock::new_from_columns(columns));
        self.finalize_partitions[partition_id].clone()
    }

    pub fn set_row_threshold(&mut self, row_threshold: usize) {
        self.rows_threshold = row_threshold;
    }

    pub fn set_bytes_threshold(&mut self, bytes_threshold: usize) {
        self.bytes_threshold = bytes_threshold;
    }

    pub fn is_partition_empty(&self, partition_id: usize) -> bool {
        self.partitions[partition_id].columns_builder[0].len() == 0
            && self.finalize_partitions[partition_id].is_none()
    }
}

pub fn copy_column<I: Index>(indices: &[I], from: &Column, to: &mut ColumnBuilder) {
    match (to, from) {
        (ColumnBuilder::Nullable(builder), Column::Null { .. }) => {
            builder.push_repeat_null(indices.len());
        }
        (ColumnBuilder::Array(builder), Column::EmptyArray { .. }) => {
            for _ in 0..indices.len() {
                builder.commit_row();
            }
        }
        (ColumnBuilder::Map(builder), Column::EmptyMap { .. }) => {
            for _ in 0..indices.len() {
                builder.commit_row();
            }
        }
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
        (ColumnBuilder::Array(builder), Column::Array(column)) => {
            copy_array(builder, column, indices);
        }
        (ColumnBuilder::Map(builder), Column::Map(column)) => {
            copy_array(builder, column, indices);
        }
        (ColumnBuilder::Nullable(builder), Column::Nullable(column)) => {
            copy_nullable(builder, column, indices);
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
        _ => unreachable!(),
    }
}

fn copy_boolean<I: Index>(to: &mut MutableBitmap, from: &Bitmap, indices: &[I]) {
    let num_rows = indices.len();
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
    // TODO:
    for index in indices {
        unsafe { to.push(from.index_unchecked(index.to_usize())) }
    }
}
