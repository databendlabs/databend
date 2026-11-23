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

use databend_common_column::binary::BinaryColumn;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;

use crate::BlockEntry;
use crate::Column;
use crate::DataBlock;
use crate::types::AnyType;
use crate::types::ArrayColumn;
use crate::types::DecimalColumn;
use crate::types::GeographyColumn;
use crate::types::NumberColumn;
use crate::types::OpaqueColumn;
use crate::types::VectorColumn;
use crate::with_number_type;
use crate::with_opaque_type;

const GC_MINIMUM_SAVINGS: usize = 16 * 1024;

impl Column {
    /// Opportunistically compacts backing buffers when they are substantially
    /// larger than this column's visible slice, releasing dead rows retained
    /// by zero-copy slicing.
    pub fn maybe_gc(self) -> Self {
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => self,
            Column::Number(col) => Column::Number(with_number_type!(|NUM_TYPE| match col {
                NumberColumn::NUM_TYPE(buf) => NumberColumn::NUM_TYPE(maybe_gc_buffer(buf)),
            })),
            Column::Decimal(col) => Column::Decimal(match col {
                DecimalColumn::Decimal64(buf, size) => {
                    DecimalColumn::Decimal64(maybe_gc_buffer(buf), size)
                }
                DecimalColumn::Decimal128(buf, size) => {
                    DecimalColumn::Decimal128(maybe_gc_buffer(buf), size)
                }
                DecimalColumn::Decimal256(buf, size) => {
                    DecimalColumn::Decimal256(maybe_gc_buffer(buf), size)
                }
            }),
            Column::Boolean(bitmap) => Column::Boolean(maybe_gc_bitmap(bitmap)),
            Column::Binary(col) => Column::Binary(maybe_gc_binary(col)),
            Column::String(col) => Column::String(col.maybe_gc()),
            Column::Timestamp(buf) => Column::Timestamp(maybe_gc_buffer(buf)),
            Column::TimestampTz(buf) => Column::TimestampTz(maybe_gc_buffer(buf)),
            Column::Date(buf) => Column::Date(maybe_gc_buffer(buf)),
            Column::Interval(buf) => Column::Interval(maybe_gc_buffer(buf)),
            Column::Array(col) => Column::Array(Box::new(maybe_gc_array(*col))),
            Column::Map(col) => Column::Map(Box::new(maybe_gc_array(*col))),
            Column::Bitmap(col) => Column::Bitmap(maybe_gc_binary(col)),
            Column::Nullable(nullable) => {
                let (column, validity) = nullable.destructure();
                column
                    .maybe_gc()
                    .wrap_nullable(Some(maybe_gc_bitmap(validity)))
            }
            Column::Tuple(fields) => {
                Column::Tuple(fields.into_iter().map(Column::maybe_gc).collect())
            }
            Column::Variant(col) => Column::Variant(maybe_gc_binary(col)),
            Column::Geometry(col) => Column::Geometry(maybe_gc_binary(col)),
            Column::Geography(GeographyColumn(col)) => {
                Column::Geography(GeographyColumn(maybe_gc_binary(col)))
            }
            Column::Vector(col) => Column::Vector(match col {
                VectorColumn::Int8((buf, dim)) => VectorColumn::Int8((maybe_gc_buffer(buf), dim)),
                VectorColumn::Float32((buf, dim)) => {
                    VectorColumn::Float32((maybe_gc_buffer(buf), dim))
                }
            }),
            Column::Opaque(col) => Column::Opaque(with_opaque_type!(|T| match col {
                OpaqueColumn::T(buf) => OpaqueColumn::T(maybe_gc_buffer(buf)),
            })),
            Column::Encoded(mut encoded) => {
                *encoded = encoded
                    .map_codes(|codes| crate::map_narrow_buffer!(codes, |b| maybe_gc_buffer(b)));
                Column::Encoded(encoded)
            }
        }
    }
}

/// Shared backing-buffer usage aggregated across a batch of sibling blocks.
#[derive(Default)]
pub(crate) struct BackingStats {
    entries: HashMap<usize, BackingUsage>,
}

struct BackingUsage {
    backing_bytes: usize,
    live_bytes: usize,
}

impl BackingStats {
    fn record(&mut self, ptr: usize, backing_bytes: usize, live_bytes: usize) {
        self.entries
            .entry(ptr)
            .and_modify(|usage| {
                usage.backing_bytes = usage.backing_bytes.max(backing_bytes);
                usage.live_bytes = usage.live_bytes.saturating_add(live_bytes);
            })
            .or_insert(BackingUsage {
                backing_bytes,
                live_bytes,
            });
    }

    fn should_gc(&self, ptr: usize) -> bool {
        let Some(usage) = self.entries.get(&ptr) else {
            return false;
        };
        let savings = usage.backing_bytes.saturating_sub(usage.live_bytes);
        // Compact when at least 20% (> 1 - SELECTIVITY_THRESHOLD) of the shared
        // backing allocation is dead across the batch and >= 16 KiB is saved.
        savings >= GC_MINIMUM_SAVINGS
            && usage.live_bytes.saturating_mul(5) <= usage.backing_bytes.saturating_mul(4)
    }

    fn has_any_gc(&self) -> bool {
        self.entries.keys().any(|&ptr| self.should_gc(ptr))
    }
}

pub(crate) fn collect_blocks_gc_stats(blocks: &[DataBlock]) -> Option<Vec<Option<BackingStats>>> {
    if blocks.is_empty() {
        return None;
    }
    let num_columns = blocks[0].num_columns();
    if num_columns == 0 {
        return None;
    }

    let mut col_stats = Vec::with_capacity(num_columns);
    let mut any_gc = false;
    for col_idx in 0..num_columns {
        let mut stats = BackingStats::default();
        for block in blocks {
            if let BlockEntry::Column(col) = block.get_by_offset(col_idx) {
                collect_column_usage(col, &mut stats);
            }
        }
        if stats.has_any_gc() {
            any_gc = true;
            col_stats.push(Some(stats));
        } else {
            col_stats.push(None);
        }
    }

    any_gc.then_some(col_stats)
}

pub(crate) fn gc_column_with_stats(col: Column, stats: &BackingStats) -> Column {
    match col {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => col,
        Column::Number(col) => Column::Number(with_number_type!(|NUM_TYPE| match col {
            NumberColumn::NUM_TYPE(buf) => NumberColumn::NUM_TYPE(gc_buffer_with_stats(buf, stats)),
        })),
        Column::Decimal(col) => Column::Decimal(match col {
            DecimalColumn::Decimal64(buf, size) => {
                DecimalColumn::Decimal64(gc_buffer_with_stats(buf, stats), size)
            }
            DecimalColumn::Decimal128(buf, size) => {
                DecimalColumn::Decimal128(gc_buffer_with_stats(buf, stats), size)
            }
            DecimalColumn::Decimal256(buf, size) => {
                DecimalColumn::Decimal256(gc_buffer_with_stats(buf, stats), size)
            }
        }),
        Column::Boolean(bitmap) => Column::Boolean(gc_bitmap_with_stats(bitmap, stats)),
        Column::Binary(col) => Column::Binary(gc_binary_with_stats(col, stats)),
        Column::String(col) => {
            let need_gc = (col.views().is_sliced() && stats.should_gc(col.views().backing_ptr()))
                || (!col.data_buffers().is_empty() && stats.should_gc(col.buffers_ptr()));
            Column::String(if need_gc { col.gc() } else { col })
        }
        Column::Timestamp(buf) => Column::Timestamp(gc_buffer_with_stats(buf, stats)),
        Column::TimestampTz(buf) => Column::TimestampTz(gc_buffer_with_stats(buf, stats)),
        Column::Date(buf) => Column::Date(gc_buffer_with_stats(buf, stats)),
        Column::Interval(buf) => Column::Interval(gc_buffer_with_stats(buf, stats)),
        Column::Array(col) => Column::Array(Box::new(gc_array_with_stats(*col, stats))),
        Column::Map(col) => Column::Map(Box::new(gc_array_with_stats(*col, stats))),
        Column::Bitmap(col) => Column::Bitmap(gc_binary_with_stats(col, stats)),
        Column::Nullable(nullable) => {
            let (column, validity) = nullable.destructure();
            gc_column_with_stats(column, stats)
                .wrap_nullable(Some(gc_bitmap_with_stats(validity, stats)))
        }
        Column::Tuple(fields) => Column::Tuple(
            fields
                .into_iter()
                .map(|f| gc_column_with_stats(f, stats))
                .collect(),
        ),
        Column::Variant(col) => Column::Variant(gc_binary_with_stats(col, stats)),
        Column::Geometry(col) => Column::Geometry(gc_binary_with_stats(col, stats)),
        Column::Geography(GeographyColumn(col)) => {
            Column::Geography(GeographyColumn(gc_binary_with_stats(col, stats)))
        }
        Column::Vector(col) => Column::Vector(match col {
            VectorColumn::Int8((buf, dim)) => {
                VectorColumn::Int8((gc_buffer_with_stats(buf, stats), dim))
            }
            VectorColumn::Float32((buf, dim)) => {
                VectorColumn::Float32((gc_buffer_with_stats(buf, stats), dim))
            }
        }),
        Column::Opaque(col) => Column::Opaque(with_opaque_type!(|T| match col {
            OpaqueColumn::T(buf) => OpaqueColumn::T(gc_buffer_with_stats(buf, stats)),
        })),
        Column::Encoded(mut encoded) => {
            *encoded = encoded.map_codes(|codes| {
                crate::map_narrow_buffer!(codes, |b| gc_buffer_with_stats(b, stats))
            });
            Column::Encoded(encoded)
        }
    }
}

fn collect_column_usage(col: &Column, stats: &mut BackingStats) {
    match col {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
        Column::Number(col) => with_number_type!(|NUM_TYPE| match col {
            NumberColumn::NUM_TYPE(buf) => collect_buffer_usage(buf, stats),
        }),
        Column::Decimal(col) => match col {
            DecimalColumn::Decimal64(buf, _) => collect_buffer_usage(buf, stats),
            DecimalColumn::Decimal128(buf, _) => collect_buffer_usage(buf, stats),
            DecimalColumn::Decimal256(buf, _) => collect_buffer_usage(buf, stats),
        },
        Column::Boolean(bitmap) => collect_bitmap_usage(bitmap, stats),
        Column::Binary(col)
        | Column::Bitmap(col)
        | Column::Variant(col)
        | Column::Geometry(col) => {
            collect_binary_usage(col, stats);
        }
        Column::Geography(GeographyColumn(col)) => collect_binary_usage(col, stats),
        Column::String(col) => {
            let views = col.views();
            let buf_len = col.total_buffer_len();
            if views.is_sliced() {
                let backing_views = views.backing_len();
                let live_views = views.len();
                stats.record(views.backing_ptr(), backing_views * 16, live_views * 16);
                if buf_len > 0 && backing_views > 0 {
                    let live_buf =
                        (buf_len as u128 * live_views as u128 / backing_views as u128) as usize;
                    stats.record(col.buffers_ptr(), buf_len, live_buf);
                }
            } else if buf_len > 0 {
                let live_bytes = col.total_bytes_len();
                if buf_len > live_bytes {
                    stats.record(col.buffers_ptr(), buf_len, live_bytes);
                }
            }
        }
        Column::Timestamp(buf) => collect_buffer_usage(buf, stats),
        Column::TimestampTz(buf) => collect_buffer_usage(buf, stats),
        Column::Date(buf) => collect_buffer_usage(buf, stats),
        Column::Interval(buf) => collect_buffer_usage(buf, stats),
        Column::Array(col) | Column::Map(col) => collect_array_usage(col, stats),
        Column::Nullable(nullable) => {
            collect_column_usage(nullable.column(), stats);
            collect_bitmap_usage(nullable.validity(), stats);
        }
        Column::Tuple(fields) => {
            for field in fields {
                collect_column_usage(field, stats);
            }
        }
        Column::Vector(col) => match col {
            VectorColumn::Int8((buf, _)) => collect_buffer_usage(buf, stats),
            VectorColumn::Float32((buf, _)) => collect_buffer_usage(buf, stats),
        },
        Column::Opaque(col) => with_opaque_type!(|T| match col {
            OpaqueColumn::T(buf) => collect_buffer_usage(buf, stats),
        }),
        Column::Encoded(encoded) => {
            let codes = match encoded.as_ref() {
                crate::types::EncodedColumn::For(c) => c.deltas(),
                crate::types::EncodedColumn::Dict(c) => &c.keys,
            };
            match codes {
                crate::types::NarrowBuffer::U8(b) => collect_buffer_usage(b, stats),
                crate::types::NarrowBuffer::U16(b) => collect_buffer_usage(b, stats),
                crate::types::NarrowBuffer::U32(b) => collect_buffer_usage(b, stats),
                crate::types::NarrowBuffer::U64(b) => collect_buffer_usage(b, stats),
            }
        }
    }
}

fn collect_buffer_usage<T>(buf: &Buffer<T>, stats: &mut BackingStats) {
    let elem_size = std::mem::size_of::<T>();
    if elem_size > 0 && buf.is_sliced() {
        stats.record(
            buf.backing_ptr(),
            buf.backing_len() * elem_size,
            buf.len() * elem_size,
        );
    }
}

fn collect_bitmap_usage(bitmap: &Bitmap, stats: &mut BackingStats) {
    let live_bytes = bitmap.len().div_ceil(8);
    let backing_bytes = bitmap.backing_bytes_len();
    if backing_bytes > live_bytes {
        stats.record(bitmap.backing_ptr(), backing_bytes, live_bytes);
    }
}

fn collect_binary_usage(col: &BinaryColumn, stats: &mut BackingStats) {
    let first = *col.offsets().first().unwrap() as usize;
    let last = *col.offsets().last().unwrap() as usize;
    let live_data = last - first;
    let data = col.data();
    if first > 0 || data.backing_len() > live_data || data.is_sliced() {
        stats.record(data.backing_ptr(), data.backing_len(), live_data);
    }
    collect_buffer_usage(col.offsets(), stats);
}

fn collect_array_usage(array: &ArrayColumn<AnyType>, stats: &mut BackingStats) {
    let offsets = array.offsets();
    let start = *offsets.first().unwrap() as usize;
    let end = *offsets.last().unwrap() as usize;
    let total_elems = array.values().len();
    if start > 0 || end < total_elems || offsets.is_sliced() {
        let values_bytes = array.values().memory_size(false);
        let backing_bytes = values_bytes + offsets.backing_len() * 8;
        let live_elems = end - start;
        let live_values_bytes = if total_elems == 0 {
            0
        } else {
            (values_bytes as u128 * live_elems as u128 / total_elems as u128) as usize
        };
        let live_bytes = live_values_bytes + offsets.len() * 8;
        stats.record(offsets.backing_ptr(), backing_bytes, live_bytes);
    } else {
        collect_column_usage(array.values(), stats);
    }
}

fn gc_buffer<T: Clone>(buf: Buffer<T>) -> Buffer<T> {
    if buf.is_sliced() {
        buf.as_slice().to_vec().into()
    } else {
        buf
    }
}

fn maybe_gc_buffer<T: Clone>(buf: Buffer<T>) -> Buffer<T> {
    let elem_size = std::mem::size_of::<T>();
    if elem_size == 0 || !buf.is_sliced() {
        return buf;
    }
    let backing_bytes = buf.backing_len() * elem_size;
    let live_bytes = buf.len() * elem_size;
    let savings = backing_bytes.saturating_sub(live_bytes);
    if savings >= GC_MINIMUM_SAVINGS && backing_bytes >= live_bytes.saturating_mul(2) {
        gc_buffer(buf)
    } else {
        buf
    }
}

fn gc_buffer_with_stats<T: Clone>(buf: Buffer<T>, stats: &BackingStats) -> Buffer<T> {
    if buf.is_sliced() && stats.should_gc(buf.backing_ptr()) {
        gc_buffer(buf)
    } else {
        buf
    }
}

fn maybe_gc_bitmap(bitmap: Bitmap) -> Bitmap {
    let backing_bytes = bitmap.backing_bytes_len();
    let live_bytes = bitmap.len().div_ceil(8);
    let savings = backing_bytes.saturating_sub(live_bytes);
    if savings >= GC_MINIMUM_SAVINGS && backing_bytes >= live_bytes.saturating_mul(2) {
        bitmap.gc()
    } else {
        bitmap
    }
}

fn gc_bitmap_with_stats(bitmap: Bitmap, stats: &BackingStats) -> Bitmap {
    if bitmap.backing_bytes_len() > bitmap.len().div_ceil(8)
        && stats.should_gc(bitmap.backing_ptr())
    {
        bitmap.gc()
    } else {
        bitmap
    }
}

fn gc_binary(col: BinaryColumn) -> BinaryColumn {
    let first = *col.offsets().first().unwrap() as usize;
    let last = *col.offsets().last().unwrap() as usize;
    let data_has_slack =
        first > 0 || col.data().backing_len() > (last - first) || col.data().is_sliced();
    if !data_has_slack && !col.offsets().is_sliced() {
        return col;
    }
    let (data, offsets) = col.into_buffer();
    let data: Buffer<u8> = if data_has_slack {
        data[first..last].to_vec().into()
    } else {
        data
    };
    let offsets: Buffer<u64> = if first > 0 {
        let base = first as u64;
        offsets
            .iter()
            .map(|&offset| offset - base)
            .collect::<Vec<_>>()
            .into()
    } else {
        gc_buffer(offsets)
    };
    BinaryColumn::new(data, offsets)
}

fn maybe_gc_binary(col: BinaryColumn) -> BinaryColumn {
    let backing_bytes = col.data().backing_len() + col.offsets().backing_len() * 8;
    let live_bytes = col.total_bytes_len() + col.offsets().len() * 8;
    let savings = backing_bytes.saturating_sub(live_bytes);
    if savings >= GC_MINIMUM_SAVINGS && backing_bytes >= live_bytes.saturating_mul(2) {
        gc_binary(col)
    } else {
        col
    }
}

fn gc_binary_with_stats(col: BinaryColumn, stats: &BackingStats) -> BinaryColumn {
    if stats.should_gc(col.data().backing_ptr()) || stats.should_gc(col.offsets().backing_ptr()) {
        gc_binary(col)
    } else {
        col
    }
}

fn gc_column_unconditional(col: Column) -> Column {
    match col {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => col,
        Column::Number(col) => Column::Number(with_number_type!(|NUM_TYPE| match col {
            NumberColumn::NUM_TYPE(buf) => NumberColumn::NUM_TYPE(gc_buffer(buf)),
        })),
        Column::Decimal(col) => Column::Decimal(match col {
            DecimalColumn::Decimal64(buf, size) => DecimalColumn::Decimal64(gc_buffer(buf), size),
            DecimalColumn::Decimal128(buf, size) => DecimalColumn::Decimal128(gc_buffer(buf), size),
            DecimalColumn::Decimal256(buf, size) => DecimalColumn::Decimal256(gc_buffer(buf), size),
        }),
        Column::Boolean(bitmap) => Column::Boolean(bitmap.gc()),
        Column::Binary(col) => Column::Binary(gc_binary(col)),
        Column::String(col) => Column::String(col.gc()),
        Column::Timestamp(buf) => Column::Timestamp(gc_buffer(buf)),
        Column::TimestampTz(buf) => Column::TimestampTz(gc_buffer(buf)),
        Column::Date(buf) => Column::Date(gc_buffer(buf)),
        Column::Interval(buf) => Column::Interval(gc_buffer(buf)),
        Column::Array(col) => Column::Array(Box::new(gc_array(*col))),
        Column::Map(col) => Column::Map(Box::new(gc_array(*col))),
        Column::Bitmap(col) => Column::Bitmap(gc_binary(col)),
        Column::Nullable(nullable) => {
            let (column, validity) = nullable.destructure();
            gc_column_unconditional(column).wrap_nullable(Some(validity.gc()))
        }
        Column::Tuple(fields) => {
            Column::Tuple(fields.into_iter().map(gc_column_unconditional).collect())
        }
        Column::Variant(col) => Column::Variant(gc_binary(col)),
        Column::Geometry(col) => Column::Geometry(gc_binary(col)),
        Column::Geography(GeographyColumn(col)) => {
            Column::Geography(GeographyColumn(gc_binary(col)))
        }
        Column::Vector(col) => Column::Vector(match col {
            VectorColumn::Int8((buf, dim)) => VectorColumn::Int8((gc_buffer(buf), dim)),
            VectorColumn::Float32((buf, dim)) => VectorColumn::Float32((gc_buffer(buf), dim)),
        }),
        Column::Opaque(col) => Column::Opaque(with_opaque_type!(|T| match col {
            OpaqueColumn::T(buf) => OpaqueColumn::T(gc_buffer(buf)),
        })),
        Column::Encoded(mut encoded) => {
            *encoded =
                encoded.map_codes(|codes| crate::map_narrow_buffer!(codes, |b| gc_buffer(b)));
            Column::Encoded(encoded)
        }
    }
}

fn gc_array(array: ArrayColumn<AnyType>) -> ArrayColumn<AnyType> {
    let (values, offsets) = array.destructure();
    let start = *offsets.first().unwrap() as usize;
    let end = *offsets.last().unwrap() as usize;
    let values = if start > 0 || end < values.len() {
        gc_column_unconditional(values.slice(start..end))
    } else {
        gc_column_unconditional(values)
    };
    let offsets = if start > 0 {
        let base = start as u64;
        offsets
            .iter()
            .map(|&offset| offset - base)
            .collect::<Vec<_>>()
            .into()
    } else {
        gc_buffer(offsets)
    };
    ArrayColumn::new(values, offsets)
}

fn maybe_gc_array(array: ArrayColumn<AnyType>) -> ArrayColumn<AnyType> {
    let start = *array.offsets().first().unwrap() as usize;
    let end = *array.offsets().last().unwrap() as usize;
    let live_elems = end - start;
    let total_elems = array.values().len();
    if start > 0 || end < total_elems {
        let total_bytes = array.values().memory_size(false) + array.offsets().backing_len() * 8;
        let live_bytes = array.memory_size(true);
        let savings = total_bytes.saturating_sub(live_bytes);
        if savings >= GC_MINIMUM_SAVINGS
            && (total_elems >= live_elems.saturating_mul(2)
                || total_bytes >= live_bytes.saturating_mul(2))
        {
            return gc_array(array);
        }
    }
    let (values, offsets) = array.destructure();
    ArrayColumn::new(values.maybe_gc(), maybe_gc_buffer(offsets))
}

fn gc_array_with_stats(array: ArrayColumn<AnyType>, stats: &BackingStats) -> ArrayColumn<AnyType> {
    let offsets = array.offsets();
    let start = *offsets.first().unwrap() as usize;
    let end = *offsets.last().unwrap() as usize;
    if (start > 0 || end < array.values().len() || offsets.is_sliced())
        && stats.should_gc(offsets.backing_ptr())
    {
        return gc_array(array);
    }
    let (values, offsets) = array.destructure();
    ArrayColumn::new(gc_column_with_stats(values, stats), offsets)
}
