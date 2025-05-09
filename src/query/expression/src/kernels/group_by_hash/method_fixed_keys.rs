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

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Not;

use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_hashtable::FastHash;
use ethnum::u256;
use ethnum::U256;
use micromarshal::Marshal;

use crate::types::boolean::BooleanType;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalColumn;
use crate::types::i256;
use crate::types::nullable::NullableColumn;
use crate::types::number::Number;
use crate::types::number::NumberColumn;
use crate::types::DataType;
use crate::types::DecimalDataType;
use crate::types::NumberDataType;
use crate::types::NumberType;
use crate::types::ValueType;
use crate::with_decimal_mapped_type;
use crate::with_integer_mapped_type;
use crate::with_number_mapped_type;
use crate::Column;
use crate::ColumnBuilder;
use crate::HashMethod;
use crate::InputColumns;
use crate::KeyAccessor;
use crate::KeysState;

pub type HashMethodKeysU8 = HashMethodFixedKeys<u8>;
pub type HashMethodKeysU16 = HashMethodFixedKeys<u16>;
pub type HashMethodKeysU32 = HashMethodFixedKeys<u32>;
pub type HashMethodKeysU64 = HashMethodFixedKeys<u64>;
pub type HashMethodKeysU128 = HashMethodFixedKeys<u128>;
pub type HashMethodKeysU256 = HashMethodFixedKeys<u256>;

#[derive(Clone, Debug)]
pub struct HashMethodFixedKeys<T> {
    t: PhantomData<T>,
}

impl<T> HashMethodFixedKeys<T>
where T: Number
{
    #[inline]
    pub fn get_key(&self, column: &Buffer<T>, row: usize) -> T {
        column[row]
    }
}

impl<T> HashMethodFixedKeys<T>
where T: Clone + Default
{
    fn build_keys_vec(&self, group_columns: InputColumns, rows: usize) -> Result<Vec<T>> {
        let mut group_columns = group_columns.iter().collect::<Vec<_>>();
        group_columns.sort_by_key(|col| {
            col.data_type()
                .remove_nullable()
                .numeric_byte_size()
                .unwrap()
        });

        let mut keys_vec = KeysVec::new::<T>(&group_columns, rows);
        for (i, col) in group_columns.into_iter().enumerate() {
            debug_assert_eq!(rows, col.len());

            fixed_hash(&mut keys_vec, i, col)?;
        }

        Ok(keys_vec.group_keys())
    }
}

impl<T> Default for HashMethodFixedKeys<T>
where T: Clone
{
    fn default() -> Self {
        HashMethodFixedKeys { t: PhantomData }
    }
}

impl<T> HashMethodFixedKeys<T>
where T: Clone
{
    pub fn deserialize_group_columns(
        &self,
        keys: Vec<T>,
        group_items: &[(usize, DataType)],
    ) -> Result<Vec<Column>> {
        debug_assert!(!keys.is_empty());

        // faster path for single signed/unsigned integer to column
        match group_items {
            [(_, DataType::Number(ty))] => {
                with_integer_mapped_type!(|NUM_TYPE| match ty {
                    NumberDataType::NUM_TYPE => {
                        let buffer: Buffer<T> = keys.into();
                        let col: Buffer<NUM_TYPE> = unsafe { std::mem::transmute(buffer) };
                        return Ok(vec![NumberType::<NUM_TYPE>::upcast_column(col)]);
                    }
                    _ => {}
                })
            }
            [(_, DataType::Decimal(decimal))] => {
                with_decimal_mapped_type!(|DECIMAL_TYPE| match decimal {
                    DecimalDataType::DECIMAL_TYPE(size) => {
                        let buffer: Buffer<T> = keys.into();
                        let col: Buffer<DECIMAL_TYPE> = unsafe { std::mem::transmute(buffer) };
                        return Ok(vec![DECIMAL_TYPE::upcast_column(col, *size)]);
                    }
                })
            }
            _ => (),
        }

        let mut keys = keys;
        let rows = keys.len();
        let step = std::mem::size_of::<T>();
        let length = rows * step;
        let capacity = keys.capacity() * step;
        let mutptr = keys.as_mut_ptr() as *mut u8;
        let vec8 = unsafe {
            std::mem::forget(keys);
            // construct new vec
            Vec::from_raw_parts(mutptr, length, capacity)
        };

        let mut res = Vec::with_capacity(group_items.len());
        let mut offsize = 0;

        let mut null_offsize = group_items
            .iter()
            .map(|(_, ty)| ty.remove_nullable().numeric_byte_size().unwrap())
            .sum::<usize>();

        let mut sorted_group_items = group_items.to_vec();
        sorted_group_items.sort_by(|(_, a), (_, b)| {
            let a = a.remove_nullable();
            let b = b.remove_nullable();
            b.numeric_byte_size()
                .unwrap()
                .cmp(&a.numeric_byte_size().unwrap())
        });

        for (_, data_type) in sorted_group_items.iter() {
            let non_null_type = data_type.remove_nullable();
            let mut column = ColumnBuilder::with_capacity(&non_null_type, rows);
            let reader = vec8.as_slice();

            let col = match data_type.is_nullable() {
                false => {
                    column.push_fix_len_binaries(&reader[offsize..], step, rows)?;
                    column.build()
                }

                true => {
                    let mut bitmap_column = ColumnBuilder::with_capacity(&DataType::Boolean, rows);
                    bitmap_column.push_fix_len_binaries(&reader[null_offsize..], step, rows)?;

                    null_offsize += 1;

                    let col = bitmap_column.build();
                    let col = BooleanType::try_downcast_column(&col).unwrap();

                    // we store 1 for nulls in fixed_hash
                    let bitmap = col.not();
                    column.push_fix_len_binaries(&reader[offsize..], step, rows)?;
                    let inner = column.build();
                    NullableColumn::new_column(inner, bitmap)
                }
            };

            offsize += non_null_type.numeric_byte_size()?;
            res.push(col);
        }

        // sort back
        let mut result_columns = Vec::with_capacity(res.len());
        for (index, data_type) in group_items.iter() {
            for (sf, col) in sorted_group_items.iter().zip(res.iter()) {
                if data_type == &sf.1 && index == &sf.0 {
                    result_columns.push(col.clone());
                    break;
                }
            }
        }
        Ok(result_columns)
    }
}

macro_rules! impl_hash_method_fixed_keys {
    ($dt: ident, $ty:ty, $signed_ty: ty) => {
        impl HashMethod for HashMethodFixedKeys<$ty> {
            type HashKey = $ty;
            type HashKeyIter<'a> = std::slice::Iter<'a, $ty>;

            fn name(&self) -> String {
                format!("FixedKeys{}", std::mem::size_of::<Self::HashKey>())
            }

            fn build_keys_state(
                &self,
                group_columns: InputColumns,
                rows: usize,
            ) -> Result<KeysState> {
                // faster path for single fixed keys
                if group_columns.len() == 1 {
                    let column = &group_columns[0];
                    if column.data_type().is_unsigned_numeric() {
                        return Ok(KeysState::Column(column.clone()));
                    }

                    if column.data_type().is_signed_numeric() {
                        let col = NumberType::<$signed_ty>::try_downcast_column(&column).unwrap();
                        let buffer =
                            unsafe { std::mem::transmute::<Buffer<$signed_ty>, Buffer<$ty>>(col) };
                        return Ok(KeysState::Column(NumberType::<$ty>::upcast_column(buffer)));
                    }
                }

                let keys = self.build_keys_vec(group_columns, rows)?;
                let col = Buffer::<$ty>::from(keys);
                Ok(KeysState::Column(NumberType::<$ty>::upcast_column(col)))
            }

            #[inline]
            fn build_keys_iter<'a>(
                &self,
                key_state: &'a KeysState,
            ) -> Result<Self::HashKeyIter<'a>> {
                use crate::types::ArgType;
                match key_state {
                    KeysState::Column(Column::Number(NumberColumn::$dt(col))) => Ok(col.iter()),
                    other => unreachable!("{:?} -> {}", other, NumberType::<$ty>::data_type()),
                }
            }

            fn build_keys_accessor(
                &self,
                keys_state: KeysState,
            ) -> Result<Box<dyn KeyAccessor<Key = Self::HashKey>>> {
                use crate::types::ArgType;
                match keys_state {
                    KeysState::Column(Column::Number(NumberColumn::$dt(col))) => {
                        Ok(Box::new(PrimitiveKeyAccessor::<$ty>::new(col)))
                    }
                    other => unreachable!("{:?} -> {}", other, NumberType::<$ty>::data_type()),
                }
            }

            fn build_keys_hashes(&self, keys_state: &KeysState, hashes: &mut Vec<u64>) {
                use crate::types::ArgType;
                match keys_state {
                    KeysState::Column(Column::Number(NumberColumn::$dt(col))) => {
                        hashes.extend(col.iter().map(|key| key.fast_hash()));
                    }
                    other => unreachable!("{:?} -> {}", other, NumberType::<$ty>::data_type()),
                }
            }
        }
    };
}

impl_hash_method_fixed_keys! {UInt8, u8, i8}
impl_hash_method_fixed_keys! {UInt16, u16, i16}
impl_hash_method_fixed_keys! {UInt32, u32, i32}
impl_hash_method_fixed_keys! {UInt64, u64, i64}

macro_rules! impl_hash_method_fixed_large_keys {
    ($ty:ty, $name: ident) => {
        impl HashMethod for HashMethodFixedKeys<$ty> {
            type HashKey = $ty;

            type HashKeyIter<'a> = std::slice::Iter<'a, $ty>;

            fn name(&self) -> String {
                format!("FixedKeys{}", std::mem::size_of::<Self::HashKey>())
            }

            fn build_keys_state(
                &self,
                group_columns: InputColumns,
                rows: usize,
            ) -> Result<KeysState> {
                // faster path for single fixed decimal keys
                if group_columns.len() == 1 {
                    if group_columns[0].data_type().is_decimal() {
                        with_decimal_mapped_type!(|DECIMAL_TYPE| match &group_columns[0] {
                            Column::Decimal(DecimalColumn::DECIMAL_TYPE(c, _)) => {
                                let buffer = unsafe {
                                    std::mem::transmute::<Buffer<DECIMAL_TYPE>, Buffer<$ty>>(
                                        c.clone(),
                                    )
                                };
                                return Ok(KeysState::$name(buffer));
                            }
                            _ => {}
                        })
                    }
                }

                let keys = self.build_keys_vec(group_columns, rows)?;
                Ok(KeysState::$name(keys.into()))
            }

            fn build_keys_iter<'a>(
                &self,
                key_state: &'a KeysState,
            ) -> Result<Self::HashKeyIter<'a>> {
                match key_state {
                    KeysState::$name(v) => Ok(v.iter()),
                    _ => unreachable!(),
                }
            }

            fn build_keys_accessor(
                &self,
                keys_state: KeysState,
            ) -> Result<Box<dyn KeyAccessor<Key = Self::HashKey>>> {
                match keys_state {
                    KeysState::$name(v) => Ok(Box::new(PrimitiveKeyAccessor::<$ty>::new(v))),
                    _ => unreachable!(),
                }
            }

            fn build_keys_hashes(&self, keys_state: &KeysState, hashes: &mut Vec<u64>) {
                match keys_state {
                    KeysState::$name(v) => {
                        hashes.extend(v.iter().map(|key| key.fast_hash()));
                    }
                    _ => unreachable!(),
                }
            }
        }
    };
}

impl_hash_method_fixed_large_keys! {u128, U128}
impl_hash_method_fixed_large_keys! {U256, U256}

// Group by (nullable(u16), nullable(u8)) needs 2 + 1 + 1 + 1 = 5 bytes, then we pad the bytes up to u64 to store the hash value.
// If the value is null, we write 1 to the null_offset, otherwise we write 0.
// since most value is not null, so this can make the hash number as low as possible.
//
// u16 column pos        u8 column pos
// │                     │                       ┌─ null offset of u8 column
// ▼                     ▼                       ▼
// ┌──────────┬──────────┬───────────┬───────────┬───────────┬───────────┬───────────┬───────────┐
// │   1byte  │   1byte  │   1byte   │   1byte   │   1byte   │   1byte   │   .....   │   1byte   │
// └──────────┴──────────┴───────────┴───────────┴───────────┼───────────┴───────────┴───────────┤
//                                   ▲                       │                                   │
//                                   │                       └─────────► unused bytes  ◄─────────┘
//                                   └─ null offset of u16 column

struct KeysVec {
    data: Vec<u8>,
    rows: usize,
    step: usize,
    column_offsets: Vec<usize>, // len = group_columns.len+1
    null_offsets: Vec<usize>,
}

impl KeysVec {
    fn new<T>(group_columns: &[&Column], rows: usize) -> Self
    where T: Clone + Default {
        let group_keys: Vec<T> = vec![T::default(); rows];
        let (ptr, length, capacity) = group_keys.into_raw_parts();
        let step = std::mem::size_of::<T>();
        let group_keys =
            unsafe { Vec::from_raw_parts(ptr as *mut u8, length * step, capacity * step) };

        let mut offset = 0;
        let column_offsets = std::iter::once(0)
            .chain(group_columns.iter().map(|col| {
                col.data_type()
                    .remove_nullable()
                    .numeric_byte_size()
                    .unwrap()
            }))
            .map(|size| {
                offset += size;
                offset
            })
            .collect::<Vec<_>>();

        let mut null_offset = *column_offsets.last().unwrap();
        assert!(null_offset <= step, "size of T too small");

        let null_offsets = group_columns
            .iter()
            .map(|col| {
                if col.data_type().is_nullable() {
                    let o = null_offset;
                    null_offset += 1;
                    o
                } else {
                    usize::MAX
                }
            })
            .collect::<Vec<_>>();

        assert!(
            null_offsets
                .iter()
                .rev()
                .find(|x| **x != usize::MAX)
                .copied()
                .unwrap_or_default()
                < step,
            "size of T too small"
        );

        Self {
            data: group_keys,
            rows,
            step,
            column_offsets,
            null_offsets,
        }
    }

    #[inline]
    fn value(&mut self, row: usize, col: usize) -> &mut [u8] {
        debug_assert!(row < self.rows);
        debug_assert!(col + 1 < self.column_offsets.len());
        let row_start = row * self.step;
        let start = unsafe { self.column_offsets.get_unchecked(col) } + row_start;
        let end = unsafe { self.column_offsets.get_unchecked(col + 1) } + row_start;
        &mut self.data[start..end]
    }

    #[inline]
    fn set_null(&mut self, row: usize, col: usize) {
        debug_assert!(row < self.rows);
        debug_assert!(col < self.column_offsets.len());
        let offset = unsafe { *self.null_offsets.get_unchecked(col) };
        debug_assert!(offset != usize::MAX);
        let offset = offset + row * self.step;
        debug_assert!(offset < self.data.len());
        unsafe { *self.data.get_unchecked_mut(offset) = 1 }
    }

    fn group_keys<T>(self) -> Vec<T> {
        assert_eq!(self.step, std::mem::size_of::<T>());
        let (ptr, length, capacity) = self.data.into_raw_parts();
        unsafe { Vec::from_raw_parts(ptr as *mut T, length / self.step, capacity / self.step) }
    }
}

fn fixed_hash(keys_vec: &mut KeysVec, col_index: usize, column: &Column) -> Result<()> {
    let (column, bitmap) = match column {
        Column::Nullable(box column) => (&column.column, Some(&column.validity)),
        column => (column, None),
    };

    match column {
        Column::Boolean(c) => match bitmap {
            Some(bitmap) => {
                for (row, (value, valid)) in c.iter().zip(bitmap.iter()).enumerate() {
                    if !valid {
                        keys_vec.set_null(row, col_index);
                        continue;
                    }
                    let slice = keys_vec.value(row, col_index);
                    slice[0] = if value { 1 } else { 0 };
                }
            }
            None => {
                for (row, value) in c.iter().enumerate() {
                    let slice = keys_vec.value(row, col_index);
                    slice[0] = if value { 1 } else { 0 };
                }
            }
        },
        Column::Number(c) => {
            with_number_mapped_type!(|NUM_TYPE| match c {
                NumberColumn::NUM_TYPE(c) => {
                    match bitmap {
                        Some(bitmap) => {
                            for (row, (value, valid)) in c.iter().zip(bitmap.iter()).enumerate() {
                                if valid {
                                    let slice = keys_vec.value(row, col_index);
                                    value.marshal(slice);
                                } else {
                                    keys_vec.set_null(row, col_index);
                                }
                            }
                        }
                        None => {
                            for (row, value) in c.iter().enumerate() {
                                let slice = keys_vec.value(row, col_index);
                                value.marshal(slice);
                            }
                        }
                    }
                }
            })
        }
        Column::Date(c) => match bitmap {
            Some(bitmap) => {
                for (row, (value, valid)) in c.iter().zip(bitmap.iter()).enumerate() {
                    if valid {
                        let slice = keys_vec.value(row, col_index);
                        value.marshal(slice);
                    } else {
                        keys_vec.set_null(row, col_index);
                    }
                }
            }
            None => {
                for (row, value) in c.iter().enumerate() {
                    let slice = keys_vec.value(row, col_index);
                    value.marshal(slice);
                }
            }
        },
        Column::Timestamp(c) => match bitmap {
            Some(bitmap) => {
                for (row, (value, valid)) in c.iter().zip(bitmap.iter()).enumerate() {
                    if valid {
                        let slice = keys_vec.value(row, col_index);
                        value.marshal(slice);
                    } else {
                        keys_vec.set_null(row, col_index);
                    }
                }
            }
            None => {
                for (row, value) in c.iter().enumerate() {
                    let slice = keys_vec.value(row, col_index);
                    value.marshal(slice);
                }
            }
        },
        Column::Decimal(c) => {
            with_decimal_mapped_type!(|DECIMAL_TYPE| match c {
                DecimalColumn::DECIMAL_TYPE(c, _) => {
                    match bitmap {
                        Some(bitmap) => {
                            for (row, (value, valid)) in c.iter().zip(bitmap.iter()).enumerate() {
                                if valid {
                                    let slice = keys_vec.value(row, col_index);
                                    value.marshal(slice);
                                } else {
                                    keys_vec.set_null(row, col_index);
                                }
                            }
                        }
                        None => {
                            for (row, value) in c.iter().enumerate() {
                                let slice = keys_vec.value(row, col_index);
                                value.marshal(slice);
                            }
                        }
                    }
                }
            })
        }
        _ => {
            return Err(ErrorCode::BadDataValueType(format!(
                "Unsupported apply fn fixed_hash operation for column: {:?}",
                column.data_type()
            )));
        }
    }

    Ok(())
}

pub struct PrimitiveKeyAccessor<T> {
    data: Buffer<T>,
}

impl<T> PrimitiveKeyAccessor<T> {
    pub fn new(data: Buffer<T>) -> Self {
        Self { data }
    }
}

impl<T> KeyAccessor for PrimitiveKeyAccessor<T> {
    type Key = T;

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key {
        self.data.get_unchecked(index)
    }
}
