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

use common_arrow::arrow::bitmap::Bitmap;
use ethnum::i256;

use super::keys_ref::KeysRef;
use crate::kernels::utils::copy_aligned;
use crate::kernels::utils::copy_aligned_advance;
use crate::kernels::utils::set_vec_len_by_ptr;
use crate::kernels::utils::store;
use crate::kernels::utils::store_advance;
use crate::kernels::utils::store_aligned_advance;
use crate::types::decimal::DecimalColumn;
use crate::types::string::StringColumn;
use crate::types::NumberColumn;
use crate::with_decimal_mapped_type;
use crate::with_number_mapped_type;
use crate::Column;

/// The serialize_size is equal to the number of bytes required by serialization.
pub fn serialize_columns(
    columns: &[Column],
    num_rows: usize,
    serialize_size: usize,
) -> StringColumn {
    // [`StringColumn`] consists of [`data`] and [`offset`], we build [`data`] and [`offset`] respectively,
    // and then call `StringColumn::new(data.into(), offsets.into())` to create [`StringColumn`].
    let mut data: Vec<u8> = Vec::with_capacity(serialize_size);
    let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
    let mut data_ptr = data.as_mut_ptr();
    let mut offsets_ptr = offsets.as_mut_ptr();
    let mut offset = 0;

    unsafe {
        store_aligned_advance::<u64>(0, &mut offsets_ptr);
        for i in 0..num_rows {
            let old_ptr = data_ptr;
            for col in columns.iter() {
                serialize_column(col, i, &mut data_ptr);
            }
            offset += data_ptr as u64 - old_ptr as u64;
            store_aligned_advance::<u64>(offset, &mut offsets_ptr);
        }
        set_vec_len_by_ptr(&mut data, data_ptr);
        set_vec_len_by_ptr(&mut offsets, offsets_ptr);
    }

    StringColumn::new(data.into(), offsets.into())
}

/// This function must be consistent with the `push_binary` function of `src/query/expression/src/values.rs`.
/// # Safety
///
/// * The size of the memory pointed by `row_space` is equal to the number of bytes required by serialization.
unsafe fn serialize_column(column: &Column, row: usize, row_space: &mut *mut u8) {
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
        Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberColumn::NUM_TYPE(v) => {
                store_advance::<NUM_TYPE>(&v[row], row_space);
            }
        }),
        Column::Decimal(v) => {
            with_decimal_mapped_type!(|DECIMAL_TYPE| match v {
                DecimalColumn::DECIMAL_TYPE(v, _) => {
                    store_advance::<DECIMAL_TYPE>(&v[row], row_space);
                }
            })
        }
        Column::Boolean(v) => store_advance::<bool>(&v.get_bit(row), row_space),
        Column::String(v) | Column::Bitmap(v) | Column::Variant(v) => {
            let value = unsafe { v.index_unchecked(row) };
            let len = value.len();
            store_advance::<u64>(&(len as u64), row_space);
            copy_aligned_advance::<u8>(value.as_ptr(), row_space, len);
        }
        Column::Timestamp(v) => store_advance::<i64>(&v[row], row_space),
        Column::Date(v) => store_advance::<i32>(&v[row], row_space),
        Column::Array(array) | Column::Map(array) => {
            let data = array.index(row).unwrap();
            store_advance::<u64>(&(data.len() as u64), row_space);
            for i in 0..data.len() {
                serialize_column(&data, i, row_space);
            }
        }
        Column::Nullable(c) => {
            let valid = c.validity.get_bit(row);
            store_advance::<bool>(&valid, row_space);
            if valid {
                serialize_column(&c.column, row, row_space);
            }
        }
        Column::Tuple(fields) => {
            for inner_col in fields.iter() {
                serialize_column(inner_col, row, row_space);
            }
        }
    }
}

/// The vectorized version of [`serialize_columns`]. Try to leverage SIMD to improve performance.
///
/// To serialize a column in batch, we need to preallocate a deterministic memory space for each row of the target column at once.
pub fn serialize_columns_vec(
    columns: &[Column],
    num_rows: usize,
    max_bytes_per_row: usize,
    buffer: &mut Vec<u8>,
) -> Vec<KeysRef> {
    // To serialize columns in batch, we need to preallocate a deterministic memory space for each row of the target column at once.
    if buffer.capacity() < num_rows * max_bytes_per_row {
        buffer.reserve(num_rows * max_bytes_per_row - buffer.capacity());
    }
    unsafe {
        // Construct the pointer of each row in `data`.
        let ptr = buffer.as_mut_ptr();
        let mut keys = (0..num_rows)
            .map(|i| KeysRef::new(ptr.add(i * max_bytes_per_row)))
            .collect();

        for col in columns {
            serialize_column_vec(&mut keys, col);
        }
        keys
    }
}

/// The vectorized version of [`serialize_column`]. Try to leverage SIMD to improve performance.
///
/// # Safety
///
/// The serialized size of each row in `column` should not exceed `column.max_serialize_size_per_row()`.
unsafe fn serialize_column_vec(dst: &mut Vec<KeysRef>, column: &Column) {
    debug_assert_eq!(dst.len(), column.len());
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
        Column::Number(col) => with_number_mapped_type!(|NUM_TYPE| match col {
            NumberColumn::NUM_TYPE(col) => {
                for (key, val) in dst.iter_mut().zip(col.iter()) {
                    store(val, key.writer(std::mem::size_of::<NUM_TYPE>()));
                }
            }
        }),
        Column::Decimal(col) => {
            with_decimal_mapped_type!(|DECIMAL_TYPE| match col {
                DecimalColumn::DECIMAL_TYPE(col, _) => {
                    for (key, val) in dst.iter_mut().zip(col.iter()) {
                        store(val, key.writer(std::mem::size_of::<DECIMAL_TYPE>()));
                    }
                }
            })
        }
        Column::Boolean(col) => {
            for (key, val) in dst.iter_mut().zip(col.iter()) {
                store(&val, key.writer(1));
            }
        }
        Column::Timestamp(col) => {
            for (key, val) in dst.iter_mut().zip(col.iter()) {
                store(val, key.writer(8));
            }
        }
        Column::Date(col) => {
            for (key, val) in dst.iter_mut().zip(col.iter()) {
                store(val, key.writer(4));
            }
        }
        Column::Nullable(col) => {
            for (key, val) in dst.iter_mut().zip(col.validity.iter()) {
                store(&val, key.writer(1));
            }
            serialize_column_vec_with_bitmap(dst, &col.column, &col.validity)
        }
        Column::String(v) | Column::Bitmap(v) | Column::Variant(v) => {
            for (key, val) in dst.iter_mut().zip(v.iter()) {
                let len = val.len();
                store(&(len as u64), key.writer(8));
                copy_aligned(val.as_ptr(), key.writer(len), len);
            }
        }
        Column::Array(array) | Column::Map(array) => {
            for (key, offsets) in dst.iter_mut().zip(array.offsets.windows(2)) {
                let len = offsets[1] - offsets[0];
                store(&len, key.writer(8));
            }
            for (key, col) in dst.iter_mut().zip(array.iter()) {
                // Degrade to serialize_column_binary.
                // We need to calculate the new length manually.
                let mut temp = key.writer(0);
                for i in 0..col.len() {
                    serialize_column(&col, i, &mut temp);
                }
                let new_len = temp as usize - key.get() as usize;
                key.set_len(new_len);
            }
        }
        Column::Tuple(fields) => {
            for inner_col in fields.iter() {
                serialize_column_vec(dst, inner_col);
            }
        }
    }
}

unsafe fn serialize_column_vec_with_bitmap(
    dst: &mut Vec<KeysRef>,
    column: &Column,
    bitmap: &Bitmap,
) {
    debug_assert_eq!(dst.len(), column.len());
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
        Column::Number(col) => with_number_mapped_type!(|NUM_TYPE| match col {
            NumberColumn::NUM_TYPE(col) => {
                for ((key, val), v) in dst.iter_mut().zip(col.iter()).zip(bitmap.iter()) {
                    if v {
                        store(val, key.writer(std::mem::size_of::<NUM_TYPE>()));
                    }
                }
            }
        }),
        Column::Decimal(col) => {
            with_decimal_mapped_type!(|DECIMAL_TYPE| match col {
                DecimalColumn::DECIMAL_TYPE(col, _) => {
                    for ((key, val), v) in dst.iter_mut().zip(col.iter()).zip(bitmap.iter()) {
                        if v {
                            store(val, key.writer(std::mem::size_of::<DECIMAL_TYPE>()));
                        }
                    }
                }
            })
        }
        Column::Boolean(col) => {
            for ((key, val), v) in dst.iter_mut().zip(col.iter()).zip(bitmap.iter()) {
                if v {
                    store(&val, key.writer(1));
                }
            }
        }
        Column::Timestamp(col) => {
            for ((key, val), v) in dst.iter_mut().zip(col.iter()).zip(bitmap.iter()) {
                if v {
                    store(&val, key.writer(8));
                }
            }
        }
        Column::Date(col) => {
            for ((key, val), v) in dst.iter_mut().zip(col.iter()).zip(bitmap.iter()) {
                if v {
                    store(&val, key.writer(4));
                }
            }
        }
        Column::Nullable(_) => {
            unreachable!("Nullable column cannot be wrapped by nullable.")
        }
        Column::String(col) | Column::Bitmap(col) | Column::Variant(col) => {
            for ((key, val), v) in dst.iter_mut().zip(col.iter()).zip(bitmap.iter()) {
                if v {
                    let len = val.len();
                    store(&(len as u64), key.writer(8));
                    copy_aligned(val.as_ptr(), key.writer(len), len);
                }
            }
        }
        Column::Array(array) | Column::Map(array) => {
            for ((key, offsets), v) in dst
                .iter_mut()
                .zip(array.offsets.windows(2))
                .zip(bitmap.iter())
            {
                if v {
                    let len = offsets[1] - offsets[0];
                    store(&len, key.writer(8));
                }
            }
            for ((key, col), v) in dst.iter_mut().zip(array.iter()).zip(bitmap.iter()) {
                if v {
                    // Degrade to serialize_column_binary.
                    // We need to calculate the new length manually.
                    let mut temp = key.writer(0);
                    for i in 0..col.len() {
                        serialize_column(&col, i, &mut temp);
                    }
                    let new_len = temp as usize - key.get() as usize;
                    key.set_len(new_len);
                }
            }
        }
        Column::Tuple(fields) => {
            for inner_col in fields.iter() {
                serialize_column_vec_with_bitmap(dst, inner_col, bitmap);
            }
        }
    }
}
