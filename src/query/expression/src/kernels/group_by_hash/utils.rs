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

use databend_common_column::types::NativeType;
use databend_common_column::types::i256 as NativeI256;

use crate::Column;
use crate::ProjectedBlock;
use crate::types::AnyType;
use crate::types::BinaryColumn;
use crate::types::EncodedColumn;
use crate::types::NullableColumn;
use crate::types::NumberColumn;
use crate::types::decimal::DecimalColumn;
use crate::types::number::Number;
use crate::types::vector::VectorScalarRef;
use crate::utils::bitmap::normalize_bitmap_column;
use crate::with_number_mapped_type;
use crate::with_vector_number_type;

/// Serialize row keys, using `serialize_size` only as an initial capacity hint.
pub fn serialize_group_columns(
    columns: ProjectedBlock,
    num_rows: usize,
    serialize_size: usize,
) -> BinaryColumn {
    let columns: Vec<Column> = columns
        .iter()
        // Decoded once per block: rows are serialized one at a time.
        .map(|entry| normalize_bitmap_in_column(entry.to_column().decode_owned()))
        .collect();
    assert!(columns.iter().all(|column| column.len() >= num_rows));
    let offset_count = num_rows.checked_add(1).expect("too many group keys");
    let mut offsets = Vec::with_capacity(offset_count);
    let mut writer = GroupKeyWriter::with_capacity(serialize_size);
    offsets.push(0);

    for row in 0..num_rows {
        for column in &columns {
            // SAFETY: each column has at least num_rows rows. The writer checks
            // capacity before every write, independently of the size estimate.
            unsafe {
                serialize_column_binary(column, row, &mut writer);
            }
        }
        offsets.push(u64::try_from(writer.len()).expect("group key offset overflow"));
    }
    BinaryColumn::new(writer.finish().into(), offsets.into())
}

/// This function must be consistent with the `push_binary` function of `src/query/expression/src/values.rs`.
/// # Safety
///
/// * `row` must be a valid row index in `column`.
unsafe fn serialize_column_binary(column: &Column, row: usize, row_space: &mut GroupKeyWriter) {
    unsafe {
        match column {
            Column::Encoded(encoded) => match encoded.as_ref() {
                EncodedColumn::Dict(c) => {
                    let key = c.keys.get_unchecked(row) as usize;
                    serialize_column_binary(&c.values, key, row_space)
                }
                EncodedColumn::For(_) => encoded.with_row(row, |column, row| {
                    serialize_column_binary(column, row, row_space)
                }),
            },
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
            Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
                NumberColumn::NUM_TYPE(v) => {
                    row_space.store(&v[row].canonicalize());
                }
            }),
            Column::Decimal(v) => match v {
                DecimalColumn::Decimal64(v, _) => row_space.store(&v[row]),
                DecimalColumn::Decimal128(v, _) => row_space.store(&v[row]),
                DecimalColumn::Decimal256(v, _) => row_space.store(&NativeI256(v[row].0)),
            },
            Column::Boolean(v) => row_space.store(&(v.get_bit(row) as u8)),
            Column::Binary(v) | Column::Bitmap(v) | Column::Variant(v) | Column::Geometry(v) => {
                let value = v.index_unchecked(row);
                row_space.store(&(value.len() as u64));
                row_space.extend(value);
            }
            Column::Geography(v) => {
                let value = v.index_unchecked(row);
                let value = borsh::to_vec(&value.0).unwrap();
                row_space.store(&(value.len() as u64));
                row_space.extend(&value);
            }
            Column::String(v) => {
                let value = v.index_unchecked(row);
                row_space.store(&(value.len() as u64));
                row_space.extend(value.as_bytes());
            }
            Column::Opaque(_v) => {
                unimplemented!()
            }
            Column::Timestamp(v) => row_space.store(&v[row]),
            Column::TimestampTz(v) => row_space.store(&v[row]),
            Column::Date(v) => row_space.store(&v[row]),
            Column::Interval(v) => row_space.store(&v[row]),
            Column::Array(array) | Column::Map(array) => {
                let data = array.index(row).unwrap();
                row_space.store(&(data.len() as u64));

                for i in 0..data.len() {
                    serialize_column_binary(&data, i, row_space);
                }
            }
            Column::Nullable(c) => {
                let valid = c.validity.get_bit(row);
                row_space.store(&(valid as u8));
                if valid {
                    serialize_column_binary(&c.column, row, row_space);
                }
            }
            Column::Tuple(fields) => {
                for inner_col in fields.iter() {
                    serialize_column_binary(inner_col, row, row_space);
                }
            }
            Column::Vector(col) => {
                let scalar = col.index_unchecked(row);
                with_vector_number_type!(|NUM_TYPE| match scalar {
                    VectorScalarRef::NUM_TYPE(vals) => {
                        for val in vals {
                            row_space.store(&val.canonicalize());
                        }
                    }
                })
            }
        }
    }
}

/// Keep the initialized prefix separate from Vec's length, updating the latter
/// only on growth and at completion rather than after every field of every row.
struct GroupKeyWriter {
    data: Vec<u8>,
    cursor: *mut u8,
    remaining: usize,
}

impl GroupKeyWriter {
    fn with_capacity(capacity: usize) -> Self {
        let mut data = Vec::with_capacity(capacity);
        let cursor = data.as_mut_ptr();
        let remaining = data.capacity();
        Self {
            data,
            cursor,
            remaining,
        }
    }

    fn len(&self) -> usize {
        self.data.capacity() - self.remaining
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        if additional > self.remaining {
            let written = self.len();
            // SAFETY: all bytes below written were initialized by store/extend.
            // Publish that prefix before reserve so it is retained on growth.
            unsafe { self.data.set_len(written) };
            // Vec checks length/capacity overflow before allocating. No pointer
            // arithmetic uses additional until the allocation is large enough.
            self.data.reserve(additional);
            // SAFETY: written is within the new allocation. Refresh the cursor
            // after a possible reallocation rather than retaining a stale pointer.
            self.cursor = unsafe { self.data.as_mut_ptr().add(written) };
            self.remaining = self.data.capacity() - written;
        }
    }

    #[inline]
    fn store<T: NativeType>(&mut self, value: &T) {
        let size = std::mem::size_of::<T>();
        self.reserve(size);
        // SAFETY: NativeType requires Pod, so value has no uninitialized bytes.
        // It cannot alias this private allocation. reserve made room for size
        // bytes, keeping the cursor at or before the allocation's end.
        unsafe {
            std::ptr::copy_nonoverlapping(value as *const T as *const u8, self.cursor, size);
            self.cursor = self.cursor.add(size);
        }
        self.remaining -= size;
    }

    #[inline]
    fn extend(&mut self, value: &[u8]) {
        self.reserve(value.len());
        // SAFETY: the source is initialized, does not alias data, and reserve
        // ensured sufficient destination capacity before copying or advancing.
        unsafe {
            std::ptr::copy_nonoverlapping(value.as_ptr(), self.cursor, value.len());
            self.cursor = self.cursor.add(value.len());
        }
        self.remaining -= value.len();
    }

    fn finish(mut self) -> Vec<u8> {
        let written = self.len();
        // SAFETY: written counts only initialized bytes and never exceeds capacity.
        unsafe { self.data.set_len(written) };
        self.data
    }
}

fn normalize_bitmap_in_column(column: Column) -> Column {
    match column {
        Column::Bitmap(col) => match normalize_bitmap_column(&col) {
            std::borrow::Cow::Borrowed(_) => Column::Bitmap(col),
            std::borrow::Cow::Owned(col) => Column::Bitmap(col),
        },
        Column::Nullable(deref!(nullable)) => {
            let (col, validity) = nullable.destructure();
            Column::Nullable(Box::new(NullableColumn::<AnyType> {
                column: normalize_bitmap_in_column(col),
                validity,
            }))
        }
        Column::Tuple(columns) => Column::Tuple(
            columns
                .into_iter()
                .map(normalize_bitmap_in_column)
                .collect(),
        ),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::GroupKeyWriter;

    #[test]
    #[should_panic(expected = "capacity overflow")]
    fn test_group_key_writer_capacity_overflow() {
        let mut writer = GroupKeyWriter::with_capacity(0);
        writer.extend(&[1]);
        // Overflow must be rejected by Vec before advancing or writing a pointer.
        writer.reserve(usize::MAX);
    }
}
