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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_hashtable::FastHash;
use ethnum::u256;
use micromarshal::Marshal;

use crate::Column;
use crate::HashMethod;
use crate::KeyAccessor;
use crate::KeysState;
use crate::ProjectedBlock;
use crate::types::AccessType;
use crate::types::ArgType;
use crate::types::DecimalDataKind;
use crate::types::DecimalView;
use crate::types::NumberType;
use crate::types::decimal::DecimalColumn;
use crate::types::i256;
use crate::types::number::Number;
use crate::types::number::NumberColumn;
use crate::with_decimal_mapped_type;
use crate::with_number_mapped_type;

pub type HashMethodKeysU8 = HashMethodFixedKeys<u8>;
pub type HashMethodKeysU16 = HashMethodFixedKeys<u16>;
pub type HashMethodKeysU32 = HashMethodFixedKeys<u32>;
pub type HashMethodKeysU64 = HashMethodFixedKeys<u64>;
pub type HashMethodKeysU128 = HashMethodFixedKeys<u128>;
pub type HashMethodKeysU256 = HashMethodFixedKeys<u256>;

#[derive(Clone, Debug, Default)]
pub struct HashMethodFixedKeys<T> {
    _t: PhantomData<T>,
}

impl<T> HashMethodFixedKeys<T>
where T: Clone + Default
{
    fn build_keys_vec(group_columns: ProjectedBlock, rows: usize) -> Result<Vec<T>> {
        let mut group_columns = group_columns
            .iter()
            .map(|entry| entry.as_column().unwrap())
            .collect::<Vec<_>>();
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

pub trait FixedKey:
    FastHash + 'static + Sized + Clone + Default + Eq + Debug + Sync + Send
{
    fn downcast(keys_state: &KeysState) -> Option<&Buffer<Self>>;

    fn downcast_owned(keys_state: KeysState) -> Option<Buffer<Self>>;

    fn upcast(buffer: Buffer<Self>) -> KeysState;

    fn single_build(_: &Column) -> Option<KeysState> {
        None
    }
}

impl<T: FixedKey> HashMethod for HashMethodFixedKeys<T> {
    type HashKey = T;
    type HashKeyIter<'a> = std::slice::Iter<'a, T>;

    fn name(&self) -> String {
        format!("FixedKeys{}", std::mem::size_of::<T>())
    }

    fn build_keys_state(&self, group_columns: ProjectedBlock, rows: usize) -> Result<KeysState> {
        // faster path for single fixed keys
        if group_columns.len() == 1 {
            if let Some(res) = T::single_build(&group_columns[0].to_column()) {
                return Ok(res);
            }
        }
        let keys = HashMethodFixedKeys::<T>::build_keys_vec(group_columns, rows)?;
        Ok(T::upcast(keys.into()))
    }

    fn build_keys_iter<'a>(&self, keys_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match T::downcast(keys_state) {
            Some(buffer) => Ok(buffer.iter()),
            None => unreachable!(),
        }
    }

    fn build_keys_accessor(
        &self,
        keys_state: KeysState,
    ) -> Result<Box<dyn KeyAccessor<Key = Self::HashKey>>> {
        let Some(buffer) = T::downcast_owned(keys_state) else {
            unreachable!()
        };
        Ok(Box::new(PrimitiveKeyAccessor::new(buffer)))
    }

    fn build_keys_hashes(&self, keys_state: &KeysState, hashes: &mut Vec<u64>) {
        if let Some(buffer) = T::downcast(keys_state) {
            hashes.extend(buffer.iter().map(|key| key.fast_hash()));
        } else {
            unreachable!("Invalid keys state for type");
        }
    }
}

macro_rules! impl_hash_method_fixed_keys {
    ($dt: ident, $ty:ty) => {
        fn downcast(keys_state: &KeysState) -> Option<&Buffer<Self>> {
            if let KeysState::Column(Column::Number(NumberColumn::$dt(col))) = keys_state {
                Some(col)
            } else {
                None
            }
        }

        fn downcast_owned(keys_state: KeysState) -> Option<Buffer<Self>> {
            if let KeysState::Column(Column::Number(NumberColumn::$dt(col))) = keys_state {
                Some(col)
            } else {
                None
            }
        }

        fn upcast(buffer: Buffer<Self>) -> KeysState {
            KeysState::Column(NumberType::<Self>::upcast_column(buffer))
        }
    };
}

impl FixedKey for u8 {
    impl_hash_method_fixed_keys!(UInt8, u8);

    fn single_build(column: &Column) -> Option<KeysState> {
        match column {
            Column::Number(NumberColumn::UInt8(_)) => Some(KeysState::Column(column.clone())),
            Column::Number(NumberColumn::Int8(buffer)) => {
                let buffer = unsafe { std::mem::transmute(buffer.clone()) };
                Some(KeysState::Column(Column::Number(
                    <u8 as Number>::upcast_column(buffer),
                )))
            }
            _ => None,
        }
    }
}

impl FixedKey for u16 {
    impl_hash_method_fixed_keys!(UInt16, u16);

    fn single_build(column: &Column) -> Option<KeysState> {
        match column {
            Column::Number(NumberColumn::UInt16(_)) => Some(KeysState::Column(column.clone())),
            Column::Number(NumberColumn::Int16(buffer)) => {
                let buffer = unsafe { std::mem::transmute(buffer.clone()) };
                Some(KeysState::Column(Column::Number(u16::upcast_column(
                    buffer,
                ))))
            }
            _ => None,
        }
    }
}

impl FixedKey for u32 {
    impl_hash_method_fixed_keys!(UInt32, u32);

    fn single_build(column: &Column) -> Option<KeysState> {
        match column {
            Column::Number(NumberColumn::UInt32(_)) => Some(KeysState::Column(column.clone())),
            Column::Number(NumberColumn::Float32(buffer)) => {
                let buffer = unsafe { std::mem::transmute(buffer.clone()) };
                Some(KeysState::Column(Column::Number(u32::upcast_column(
                    buffer,
                ))))
            }
            Column::Number(NumberColumn::Int32(buffer)) | Column::Date(buffer) => {
                let buffer = unsafe { std::mem::transmute(buffer.clone()) };
                Some(KeysState::Column(Column::Number(u32::upcast_column(
                    buffer,
                ))))
            }
            _ => None,
        }
    }
}

impl FixedKey for u64 {
    impl_hash_method_fixed_keys!(UInt64, u64);

    fn single_build(column: &Column) -> Option<KeysState> {
        match column {
            Column::Number(NumberColumn::UInt64(_)) => Some(KeysState::Column(column.clone())),
            Column::Number(NumberColumn::Float64(buffer)) => {
                let buffer = unsafe { std::mem::transmute(buffer.clone()) };
                Some(KeysState::Column(Column::Number(u64::upcast_column(
                    buffer,
                ))))
            }
            Column::Number(NumberColumn::Int64(buffer))
            | Column::Decimal(DecimalColumn::Decimal64(buffer, _))
            | Column::Timestamp(buffer) => {
                let buffer = unsafe { std::mem::transmute(buffer.clone()) };
                Some(KeysState::Column(Column::Number(u64::upcast_column(
                    buffer,
                ))))
            }
            _ => None,
        }
    }
}

macro_rules! impl_hash_method_fixed_large_keys {
    ($ty: ty, $name: ident) => {
        fn downcast(keys_state: &KeysState) -> Option<&Buffer<Self>> {
            match keys_state {
                KeysState::$name(v) => Some(v),
                _ => None,
            }
        }

        fn downcast_owned(keys_state: KeysState) -> Option<Buffer<Self>> {
            match keys_state {
                KeysState::$name(v) => Some(v),
                _ => None,
            }
        }

        fn upcast(buffer: Buffer<Self>) -> KeysState {
            KeysState::$name(buffer)
        }
    };
}

impl FixedKey for u128 {
    impl_hash_method_fixed_large_keys!(u128, U128);

    fn single_build(column: &Column) -> Option<KeysState> {
        match column {
            Column::Decimal(DecimalColumn::Decimal128(c, _)) => {
                let buffer = unsafe { std::mem::transmute(c.clone()) };
                Some(Self::upcast(buffer))
            }
            _ => None,
        }
    }
}

impl FixedKey for u256 {
    impl_hash_method_fixed_large_keys!(u256, U256);

    fn single_build(column: &Column) -> Option<KeysState> {
        match column {
            Column::Decimal(DecimalColumn::Decimal256(c, _)) => {
                let buffer = unsafe { std::mem::transmute(c.clone()) };
                Some(Self::upcast(buffer))
            }
            _ => None,
        }
    }
}

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
        Column::Decimal(decimal) => {
            with_decimal_mapped_type!(|TO| match decimal.size().data_kind() {
                DecimalDataKind::TO => {
                    with_decimal_mapped_type!(|FROM| match decimal {
                        DecimalColumn::FROM(buffer, _) => {
                            fixed_hash_decimal::<DecimalView<FROM, TO>>(
                                keys_vec, col_index, bitmap, buffer,
                            );
                        }
                    })
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

fn fixed_hash_decimal<T>(
    keys_vec: &mut KeysVec,
    col_index: usize,
    bitmap: Option<&Bitmap>,
    buffer: &T::Column,
) where
    T: AccessType,
    for<'a> T::ScalarRef<'a>: Marshal,
{
    match bitmap {
        Some(bitmap) => {
            for (row, (value, valid)) in T::iter_column(buffer).zip(bitmap.iter()).enumerate() {
                if valid {
                    let slice = keys_vec.value(row, col_index);
                    value.marshal(slice);
                } else {
                    keys_vec.set_null(row, col_index);
                }
            }
        }
        None => {
            for (row, value) in T::iter_column(buffer).enumerate() {
                let slice = keys_vec.value(row, col_index);
                value.marshal(slice);
            }
        }
    }
}

pub struct PrimitiveKeyAccessor<T: Send + Sync> {
    data: Buffer<T>,
}

impl<T: Send + Sync> PrimitiveKeyAccessor<T> {
    pub fn new(data: Buffer<T>) -> Self {
        Self { data }
    }
}

impl<T: Send + Sync> KeyAccessor for PrimitiveKeyAccessor<T> {
    type Key = T;

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key {
        unsafe { self.data.get_unchecked(index) }
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}
