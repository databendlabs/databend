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
use crate::Scalar;
use crate::types::AnyType;
use crate::types::ArrayColumn;
use crate::types::BinaryColumn;
use crate::types::DataType;
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

/// Bytes a constant takes once it is copied into a builder, as
/// `base + rows * per_64_rows / 64`.
#[derive(Clone, Copy)]
struct ExpandedSize {
    base: usize,
    per_64_rows: usize,
}

impl ExpandedSize {
    fn measure(scalar: &Scalar, data_type: &DataType) -> Self {
        let size = |rows| {
            let mut builder = ColumnBuilder::with_capacity(data_type, rows);
            copy_const(scalar, data_type, rows, &mut builder);
            builder.memory_size()
        };
        let (size_64, size_128) = (size(64), size(128));
        let per_64_rows = size_128.saturating_sub(size_64);
        ExpandedSize {
            base: size_64.saturating_sub(per_64_rows),
            per_64_rows,
        }
    }

    fn of(&self, rows: usize) -> usize {
        self.base + rows * self.per_64_rows / 64
    }
}

/// The rows of one column waiting in a partition. A column whose rows so far
/// are all the same constant stays a constant instead of being copied.
enum PendingColumn {
    /// No rows. `data_type` and `capacity` are those of the next builder.
    Empty {
        data_type: DataType,
        capacity: usize,
    },
    Const {
        scalar: Scalar,
        data_type: DataType,
        rows: usize,
        // Counted as if copied, so blocks are cut where they would be if the
        // constant had been expanded.
        size: ExpandedSize,
    },
    Builder(ColumnBuilder),
}

impl PendingColumn {
    /// Add `rows` of a constant if they can stay constant: no rows yet, or
    /// only rows of the same constant. Returns false otherwise.
    fn try_push_const(
        &mut self,
        scalar: &Scalar,
        data_type: &DataType,
        rows: usize,
        size: &mut impl FnMut() -> ExpandedSize,
    ) -> bool {
        match self {
            PendingColumn::Empty {
                data_type: expected,
                ..
            } if expected == data_type => {
                *self = PendingColumn::Const {
                    scalar: scalar.clone(),
                    data_type: data_type.clone(),
                    rows,
                    size: size(),
                };
                true
            }
            PendingColumn::Const {
                scalar: pending,
                data_type: pending_type,
                rows: pending_rows,
                ..
            } if pending == scalar && pending_type == data_type => {
                *pending_rows += rows;
                true
            }
            _ => false,
        }
    }

    fn push_column<I: Index>(&mut self, indices: &[I], column: &Column) {
        copy_column(indices, column, self.builder(indices.len()));
    }

    /// Turn the pending rows into a builder with room for `additional` more.
    fn builder(&mut self, additional: usize) -> &mut ColumnBuilder {
        match self {
            PendingColumn::Empty {
                data_type,
                capacity,
            } => {
                let builder = ColumnBuilder::with_capacity(data_type, (*capacity).max(additional));
                *self = PendingColumn::Builder(builder);
            }
            PendingColumn::Const {
                scalar,
                data_type,
                rows,
                ..
            } => {
                let mut builder = ColumnBuilder::with_capacity(data_type, *rows + additional);
                copy_const(scalar, data_type, *rows, &mut builder);
                *self = PendingColumn::Builder(builder);
            }
            PendingColumn::Builder(_) => {}
        }
        match self {
            PendingColumn::Builder(builder) => builder,
            _ => unreachable!(),
        }
    }

    fn memory_size(&self) -> usize {
        match self {
            PendingColumn::Empty { .. } => 0,
            PendingColumn::Const { rows, size, .. } => size.of(*rows),
            PendingColumn::Builder(builder) => builder.memory_size(),
        }
    }

    /// Take the pending rows. The next builder gets room for as many rows when
    /// `keep_capacity` is set.
    fn take(&mut self, keep_capacity: bool) -> BlockEntry {
        let placeholder = PendingColumn::Empty {
            data_type: DataType::Null,
            capacity: 0,
        };
        let (entry, data_type, rows) = match std::mem::replace(self, placeholder) {
            PendingColumn::Empty { data_type, .. } => {
                let entry = ColumnBuilder::with_capacity(&data_type, 0).build().into();
                (entry, data_type, 0)
            }
            PendingColumn::Const {
                scalar,
                data_type,
                rows,
                ..
            } => {
                let entry = BlockEntry::new_const_column(data_type.clone(), scalar, rows);
                (entry, data_type, rows)
            }
            PendingColumn::Builder(builder) => {
                let data_type = builder.data_type();
                let rows = builder.len();
                (builder.build().into(), data_type, rows)
            }
        };
        *self = PendingColumn::Empty {
            data_type,
            capacity: if keep_capacity { rows } else { 0 },
        };
        entry
    }
}

/// Copy `rows` of a constant into `to` row by row, as a column of the constant
/// would be copied, so the builder ends up as if the constant had been expanded.
fn copy_const(scalar: &Scalar, data_type: &DataType, rows: usize, to: &mut ColumnBuilder) {
    let column = ColumnBuilder::repeat(&scalar.as_ref(), rows, data_type).build();
    let indices = (0..rows as u32).collect::<Vec<_>>();
    copy_column(&indices, &column, to);
}

struct PartitionBlockBuilder {
    num_rows: usize,
    columns: Vec<PendingColumn>,
}

impl PartitionBlockBuilder {
    fn memory_size(&self) -> usize {
        self.columns.iter().map(PendingColumn::memory_size).sum()
    }

    fn take_block(&mut self, keep_capacity: bool) -> DataBlock {
        let columns = self
            .columns
            .iter_mut()
            .map(|column| column.take(keep_capacity))
            .collect();
        let num_rows = std::mem::take(&mut self.num_rows);
        DataBlock::new(columns, num_rows)
    }
}

pub struct BlockPartitionStream {
    initialize: bool,
    scatter_size: usize,
    rows_threshold: usize,
    bytes_threshold: usize,
    partitions: Vec<PartitionBlockBuilder>,
    // The expanded size of the last constant seen in each column, as the same
    // constant usually comes in every block.
    const_sizes: Vec<Option<(Scalar, DataType, ExpandedSize)>>,
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
            const_sizes: vec![],
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
                let columns = block
                    .columns()
                    .iter()
                    .map(|column| PendingColumn::Empty {
                        data_type: column.data_type(),
                        capacity: 0,
                    })
                    .collect();
                self.partitions.push(PartitionBlockBuilder {
                    num_rows: 0,
                    columns,
                });
            }
            self.const_sizes = vec![None; block.num_columns()];
        }

        let scatter_indices =
            DataBlock::divide_indices_by_scatter_size(&indices, self.scatter_size);

        for (partition_id, indices) in scatter_indices.iter().enumerate() {
            self.partitions[partition_id].num_rows += indices.len();
        }

        for (column_idx, entry) in block.take_columns().into_iter().enumerate() {
            match entry {
                BlockEntry::Const(scalar, data_type, num_rows) => {
                    let const_size = &mut self.const_sizes[column_idx];
                    let mut size = || {
                        if let Some((s, t, size)) = const_size.as_ref()
                            && *s == scalar
                            && *t == data_type
                        {
                            return *size;
                        }
                        let size = ExpandedSize::measure(&scalar, &data_type);
                        *const_size = Some((scalar.clone(), data_type.clone(), size));
                        size
                    };
                    // Expanded once for the partitions that already hold other
                    // rows, as every block used to be.
                    let mut expanded = None;
                    for (partition_id, indices) in scatter_indices.iter().enumerate() {
                        if indices.is_empty() {
                            continue;
                        }
                        let column = &mut self.partitions[partition_id].columns[column_idx];
                        if !column.try_push_const(&scalar, &data_type, indices.len(), &mut size) {
                            let expanded = expanded.get_or_insert_with(|| {
                                ColumnBuilder::repeat(&scalar.as_ref(), num_rows, &data_type)
                                    .build()
                            });
                            column.push_column(indices, expanded);
                        }
                    }
                }
                BlockEntry::Column(column) => {
                    for (partition_id, indices) in scatter_indices.iter().enumerate() {
                        if indices.is_empty() {
                            continue;
                        }
                        self.partitions[partition_id].columns[column_idx]
                            .push_column(indices, &column);
                    }
                }
            }
        }

        if !out_ready {
            return vec![];
        }

        let mut ready_blocks = Vec::with_capacity(self.partitions.len());
        for (id, partition) in self.partitions.iter_mut().enumerate() {
            if partition.memory_size() >= self.bytes_threshold
                || partition.num_rows >= self.rows_threshold
            {
                ready_blocks.push((id, partition.take_block(true)));
            }
        }

        ready_blocks
    }

    /// Bytes buffered in all partitions, with pending constants counted as if
    /// they had been expanded.
    pub fn memory_size(&self) -> usize {
        self.partitions
            .iter()
            .map(PartitionBlockBuilder::memory_size)
            .sum()
    }

    /// Drops every partition builder, including the capacity a flush reserves
    /// for partitions that received no rows since. Call once the stream ends.
    pub fn reset(&mut self) {
        self.initialize = false;
        self.partitions = vec![];
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

            take_blocks.push((id, partition.take_block(true)));
        }

        take_blocks
    }

    pub fn finalize_partition(&mut self, partition_id: usize) -> Option<DataBlock> {
        if !self.initialize {
            return None;
        }

        let partition = &mut self.partitions[partition_id];

        if partition.num_rows == 0 {
            return None;
        }

        Some(partition.take_block(false))
    }
}

pub fn copy_column<I: Index>(indices: &[I], from: &Column, to: &mut ColumnBuilder) {
    if let Column::Encoded(encoded) = from {
        return copy_column(indices, &encoded.decode(), to);
    }
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
    copy_boolean(&mut to.validity, from.validity(), indices);
    copy_column(indices, from.column(), &mut to.builder)
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
