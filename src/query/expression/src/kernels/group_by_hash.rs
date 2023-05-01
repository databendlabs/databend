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
use std::iter::TrustedLen;
use std::marker::PhantomData;
use std::ops::Not;
use std::ptr::NonNull;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::DictionaryKeys;
use common_hashtable::FastHash;
use common_io::prelude::BinaryWrite;
use ethnum::i256;
use ethnum::u256;
use ethnum::U256;
use micromarshal::Marshal;

use crate::types::boolean::BooleanType;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalColumn;
use crate::types::nullable::NullableColumn;
use crate::types::number::Number;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::string::StringIterator;
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

#[derive(Debug)]
pub enum KeysState {
    Column(Column),
    U128(Buffer<u128>),
    U256(Buffer<u256>),
    Dictionary {
        columns: Vec<StringColumn>,
        keys_point: Vec<NonNull<[u8]>>,
        dictionaries: Vec<DictionaryKeys>,
    },
}

unsafe impl Send for KeysState {}

unsafe impl Sync for KeysState {}

pub trait HashMethod: Clone + Sync + Send + 'static {
    type HashKey: ?Sized + Eq + FastHash + Debug;

    type HashKeyIter<'a>: Iterator<Item = &'a Self::HashKey> + TrustedLen
    where Self: 'a;

    fn name(&self) -> String;

    fn build_keys_state(
        &self,
        group_columns: &[(Column, DataType)],
        rows: usize,
    ) -> Result<KeysState>;

    fn build_keys_iter<'a>(&self, keys_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>>;
}

pub type HashMethodKeysU8 = HashMethodFixedKeys<u8>;
pub type HashMethodKeysU16 = HashMethodFixedKeys<u16>;
pub type HashMethodKeysU32 = HashMethodFixedKeys<u32>;
pub type HashMethodKeysU64 = HashMethodFixedKeys<u64>;
pub type HashMethodKeysU128 = HashMethodFixedKeys<u128>;
pub type HashMethodKeysU256 = HashMethodFixedKeys<u256>;

/// These methods are `generic` method to generate hash key,
/// that is the 'numeric' or 'binary` representation of each column value as hash key.
#[derive(Clone, Debug)]
pub enum HashMethodKind {
    Serializer(HashMethodSerializer),
    DictionarySerializer(HashMethodDictionarySerializer),
    SingleString(HashMethodSingleString),
    KeysU8(HashMethodKeysU8),
    KeysU16(HashMethodKeysU16),
    KeysU32(HashMethodKeysU32),
    KeysU64(HashMethodKeysU64),
    KeysU128(HashMethodKeysU128),
    KeysU256(HashMethodKeysU256),
}

#[macro_export]
macro_rules! with_hash_method {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [Serializer, SingleString, KeysU8, KeysU16,
            KeysU32, KeysU64, KeysU128, KeysU256, DictionarySerializer],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_join_hash_method {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [Serializer, SingleString, KeysU8, KeysU16,
            KeysU32, KeysU64, KeysU128, KeysU256],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_mappedhash_method {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [
                Serializer => HashMethodSerializer,
                SingleString => HashMethodSingleString,
                KeysU8 => HashMethodKeysU8,
                KeysU16 => HashMethodKeysU16,
                KeysU32 => HashMethodKeysU32,
                KeysU64 => HashMethodKeysU64,
                KeysU128 => HashMethodKeysU128,
                KeysU256 => HashMethodKeysU256,
                DictionarySerializer => HashMethodDictionarySerializer
            ],
            $($tail)*
        }
    }
}

impl HashMethodKind {
    pub fn name(&self) -> String {
        with_hash_method!(|T| match self {
            HashMethodKind::T(v) => v.name(),
        })
    }

    pub fn data_type(&self) -> DataType {
        match self {
            HashMethodKind::Serializer(_) => DataType::String,
            HashMethodKind::SingleString(_) => DataType::String,
            HashMethodKind::KeysU8(_) => DataType::Number(NumberDataType::UInt8),
            HashMethodKind::KeysU16(_) => DataType::Number(NumberDataType::UInt16),
            HashMethodKind::KeysU32(_) => DataType::Number(NumberDataType::UInt32),
            HashMethodKind::KeysU64(_) => DataType::Number(NumberDataType::UInt64),
            HashMethodKind::KeysU128(_) => {
                DataType::Decimal(DecimalDataType::Decimal128(i128::default_decimal_size()))
            }
            HashMethodKind::KeysU256(_) => {
                DataType::Decimal(DecimalDataType::Decimal256(i256::default_decimal_size()))
            }
            HashMethodKind::DictionarySerializer(_) => DataType::String,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HashMethodSingleString {}

impl HashMethod for HashMethodSingleString {
    type HashKey = [u8];

    type HashKeyIter<'a> = StringIterator<'a>;

    fn name(&self) -> String {
        "SingleString".to_string()
    }

    fn build_keys_state(
        &self,
        group_columns: &[(Column, DataType)],
        _rows: usize,
    ) -> Result<KeysState> {
        Ok(KeysState::Column(group_columns[0].0.clone()))
    }

    fn build_keys_iter<'a>(&self, key_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match key_state {
            KeysState::Column(Column::String(col)) => Ok(col.iter()),
            KeysState::Column(Column::Variant(col)) => Ok(col.iter()),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HashMethodSerializer {}

impl HashMethod for HashMethodSerializer {
    type HashKey = [u8];

    type HashKeyIter<'a> = StringIterator<'a>;

    fn name(&self) -> String {
        "Serializer".to_string()
    }

    fn build_keys_state(
        &self,
        group_columns: &[(Column, DataType)],
        rows: usize,
    ) -> Result<KeysState> {
        let approx_size = group_columns.len() * rows * 8;
        let mut builder = StringColumnBuilder::with_capacity(rows, approx_size);

        for row in 0..rows {
            for (col, _) in group_columns {
                serialize_column_binary(col, row, &mut builder.data);
            }
            builder.commit_row();
        }

        let col = builder.build();
        Ok(KeysState::Column(Column::String(col)))
    }

    fn build_keys_iter<'a>(&self, key_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match key_state {
            KeysState::Column(Column::String(col)) => Ok(col.iter()),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashMethodDictionarySerializer {
    pub dict_keys: usize,
}

impl HashMethod for HashMethodDictionarySerializer {
    type HashKey = DictionaryKeys;
    type HashKeyIter<'a> = std::slice::Iter<'a, DictionaryKeys>;

    fn name(&self) -> String {
        "DictionarySerializer".to_string()
    }

    fn build_keys_state(
        &self,
        group_columns: &[(Column, DataType)],
        rows: usize,
    ) -> Result<KeysState> {
        // fixed type serialize one column to dictionary
        let mut dictionary_columns = Vec::with_capacity(group_columns.len());

        for (group_column, _) in group_columns {
            if let Column::String(v) = group_column {
                debug_assert_eq!(v.len(), rows);
                dictionary_columns.push(v.clone());
            } else if let Column::Variant(v) = group_column {
                debug_assert_eq!(v.len(), rows);
                dictionary_columns.push(v.clone());
            }
        }

        if dictionary_columns.len() != group_columns.len() {
            let approx_size = group_columns.len() * rows * 8;
            let mut builder = StringColumnBuilder::with_capacity(rows, approx_size);

            for row in 0..rows {
                for (group_column, _) in group_columns {
                    if !matches!(group_column, Column::String(_) | Column::Variant(_)) {
                        serialize_column_binary(group_column, row, &mut builder.data);
                    }
                }

                builder.commit_row();
            }

            dictionary_columns.push(builder.build());
        }

        let mut keys = Vec::with_capacity(rows * dictionary_columns.len());
        let mut points = Vec::with_capacity(rows * dictionary_columns.len());

        for row in 0..rows {
            let start = points.len();

            for dictionary_column in &dictionary_columns {
                points.push(NonNull::from(unsafe {
                    dictionary_column.index_unchecked(row)
                }));
            }

            keys.push(DictionaryKeys::create(&points[start..]))
        }

        Ok(KeysState::Dictionary {
            dictionaries: keys,
            keys_point: points,
            columns: dictionary_columns,
        })
    }

    fn build_keys_iter<'a>(&self, keys_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match keys_state {
            KeysState::Dictionary { dictionaries, .. } => Ok(dictionaries.iter()),
            _ => unreachable!(),
        }
    }
}

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
    fn build_keys_vec(&self, group_columns: &[(Column, DataType)], rows: usize) -> Result<Vec<T>> {
        let step = std::mem::size_of::<T>();
        let mut group_keys: Vec<T> = vec![T::default(); rows];
        let ptr = group_keys.as_mut_ptr() as *mut u8;
        let mut offsize = 0;
        let mut null_offsize = group_columns
            .iter()
            .map(|(_, t)| t.remove_nullable().numeric_byte_size().unwrap())
            .sum::<usize>();

        let mut group_columns = group_columns.to_vec();
        group_columns.sort_by(|a, b| {
            let ta = a.1.remove_nullable();
            let tb = b.1.remove_nullable();

            tb.numeric_byte_size()
                .unwrap()
                .cmp(&ta.numeric_byte_size().unwrap())
        });

        for (col, ty) in group_columns.iter() {
            build(&mut offsize, &mut null_offsize, col, ty, ptr, step)?;
        }

        Ok(group_keys)
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
        if group_items.len() == 1 {
            if let DataType::Number(ty) = group_items[0].1 {
                with_integer_mapped_type!(|NUM_TYPE| match ty {
                    NumberDataType::NUM_TYPE => {
                        let buffer: Buffer<T> = keys.into();
                        let col =
                            unsafe { std::mem::transmute::<Buffer<T>, Buffer<NUM_TYPE>>(buffer) };
                        return Ok(vec![NumberType::<NUM_TYPE>::upcast_column(col)]);
                    }
                    _ => {}
                })
            }

            if matches!(group_items[0].1, DataType::Decimal(_)) {
                with_decimal_mapped_type!(|DECIMAL_TYPE| match group_items[0].1 {
                    DataType::Decimal(DecimalDataType::DECIMAL_TYPE(size)) => {
                        let buffer: Buffer<T> = keys.into();
                        let col = unsafe {
                            std::mem::transmute::<Buffer<T>, Buffer<DECIMAL_TYPE>>(buffer)
                        };
                        return Ok(vec![DECIMAL_TYPE::upcast_column(col, size)]);
                    }
                    _ => {}
                })
            }
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
                    Column::Nullable(Box::new(NullableColumn {
                        column: inner,
                        validity: bitmap,
                    }))
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
                group_columns: &[(Column, DataType)],
                rows: usize,
            ) -> Result<KeysState> {
                // faster path for single fixed keys
                if group_columns.len() == 1 {
                    if group_columns[0].1.is_unsigned_numeric() {
                        return Ok(KeysState::Column(group_columns[0].0.clone()));
                    }

                    if group_columns[0].1.is_signed_numeric() {
                        let col =
                            NumberType::<$signed_ty>::try_downcast_column(&group_columns[0].0)
                                .unwrap();
                        let buffer =
                            unsafe { std::mem::transmute::<Buffer<$signed_ty>, Buffer<$ty>>(col) };
                        return Ok(KeysState::Column(NumberType::<$ty>::upcast_column(buffer)));
                    }
                }

                let keys = self.build_keys_vec(group_columns, rows)?;
                let col = Buffer::<$ty>::from(keys);
                Ok(KeysState::Column(NumberType::<$ty>::upcast_column(col)))
            }

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
                group_columns: &[(Column, DataType)],
                rows: usize,
            ) -> Result<KeysState> {
                // faster path for single fixed decimal keys
                if group_columns.len() == 1 {
                    if group_columns[0].1.is_decimal() {
                        with_decimal_mapped_type!(|DECIMAL_TYPE| match &group_columns[0].0 {
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
        }
    };
}

impl_hash_method_fixed_large_keys! {u128, U128}
impl_hash_method_fixed_large_keys! {U256, U256}

#[inline]
fn build(
    offsize: &mut usize,
    null_offsize: &mut usize,
    col: &Column,
    ty: &DataType,
    writer: *mut u8,
    step: usize,
) -> Result<()> {
    let data_type_nonull = ty.remove_nullable();
    let size = data_type_nonull.numeric_byte_size().unwrap();

    let writer = unsafe { writer.add(*offsize) };
    let nulls: Option<(usize, Option<Bitmap>)> = if ty.is_nullable() {
        // origin_ptr-------ptr<------old------>null_offset
        let null_offsize_from_ptr = *null_offsize - *offsize;
        *null_offsize += 1;
        Some((null_offsize_from_ptr, None))
    } else {
        None
    };

    fixed_hash(col, ty, writer, step, nulls)?;
    *offsize += size;
    Ok(())
}

pub fn serialize_column_binary(column: &Column, row: usize, vec: &mut Vec<u8>) {
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => vec.push(0),
        Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberColumn::NUM_TYPE(v) => vec.extend_from_slice(v[row].to_le_bytes().as_ref()),
        }),
        Column::Boolean(v) => vec.push(v.get_bit(row) as u8),
        Column::String(v) => {
            BinaryWrite::write_binary(vec, unsafe { v.index_unchecked(row) }).unwrap()
        }
        Column::Decimal(_) => {
            with_decimal_mapped_type!(|DECIMAL_TYPE| match column {
                Column::Decimal(DecimalColumn::DECIMAL_TYPE(v, _)) =>
                    vec.extend_from_slice(v[row].to_le_bytes().as_ref()),
                _ => unreachable!(),
            })
        }
        Column::Timestamp(v) => vec.extend_from_slice(v[row].to_le_bytes().as_ref()),
        Column::Date(v) => vec.extend_from_slice(v[row].to_le_bytes().as_ref()),
        Column::Array(array) | Column::Map(array) => {
            let data = array.index(row).unwrap();
            BinaryWrite::write_uvarint(vec, data.len() as u64).unwrap();
            for i in 0..data.len() {
                serialize_column_binary(&data, i, vec);
            }
        }
        Column::Bitmap(v) => {
            BinaryWrite::write_binary(vec, unsafe { v.index_unchecked(row) }).unwrap()
        }
        Column::Nullable(c) => {
            let valid = c.validity.get_bit(row);
            vec.push(valid as u8);
            if valid {
                serialize_column_binary(&c.column, row, vec);
            }
        }
        Column::Tuple(fields) => {
            for inner_col in fields.iter() {
                serialize_column_binary(inner_col, row, vec);
            }
        }
        Column::Variant(v) => {
            BinaryWrite::write_binary(vec, unsafe { v.index_unchecked(row) }).unwrap()
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

pub fn fixed_hash(
    column: &Column,
    data_type: &DataType,
    ptr: *mut u8,
    step: usize,
    // (null_offset, bitmap)
    nulls: Option<(usize, Option<Bitmap>)>,
) -> Result<()> {
    if let Column::Nullable(c) = column {
        let (null_offset, bitmap) = nulls.unwrap();
        let bitmap = bitmap
            .map(|b| (&b) & (&c.validity))
            .unwrap_or_else(|| c.validity.clone());

        let inner_type = data_type.as_nullable().unwrap();
        return fixed_hash(
            &c.column,
            inner_type.as_ref(),
            ptr,
            step,
            Some((null_offset, Some(bitmap))),
        );
    }

    match column {
        Column::Boolean(c) => {
            let mut ptr = ptr;
            match nulls {
                Some((offsize, Some(bitmap))) => {
                    for (value, valid) in c.iter().zip(bitmap.iter()) {
                        unsafe {
                            if valid {
                                std::ptr::copy_nonoverlapping(&(value as u8) as *const u8, ptr, 1);
                            } else {
                                ptr.add(offsize).write(1u8);
                            }
                            ptr = ptr.add(step);
                        }
                    }
                }
                _ => {
                    for value in c.iter() {
                        unsafe {
                            std::ptr::copy_nonoverlapping(&(value as u8) as *const u8, ptr, 1);
                            ptr = ptr.add(step);
                        }
                    }
                }
            }
        }
        Column::Number(c) => {
            with_number_mapped_type!(|NUM_TYPE| match c {
                NumberColumn::NUM_TYPE(t) => {
                    let mut ptr = ptr;
                    let count = std::mem::size_of::<NUM_TYPE>();
                    match nulls {
                        Some((offsize, Some(bitmap))) => {
                            for (value, valid) in t.iter().zip(bitmap.iter()) {
                                unsafe {
                                    if valid {
                                        let slice = std::slice::from_raw_parts_mut(ptr, count);
                                        value.marshal(slice);
                                    } else {
                                        ptr.add(offsize).write(1u8);
                                    }

                                    ptr = ptr.add(step);
                                }
                            }
                        }
                        _ => {
                            for value in t.iter() {
                                unsafe {
                                    let slice = std::slice::from_raw_parts_mut(ptr, count);
                                    value.marshal(slice);
                                    ptr = ptr.add(step);
                                }
                            }
                        }
                    }
                }
            })
        }
        Column::Date(c) => {
            let mut ptr = ptr;
            match nulls {
                Some((offsize, Some(bitmap))) => {
                    for (value, valid) in c.iter().zip(bitmap.iter()) {
                        unsafe {
                            if valid {
                                let slice = std::slice::from_raw_parts_mut(ptr, 4);
                                value.marshal(slice);
                            } else {
                                ptr.add(offsize).write(1u8);
                            }

                            ptr = ptr.add(step);
                        }
                    }
                }
                _ => {
                    for value in c.iter() {
                        unsafe {
                            let slice = std::slice::from_raw_parts_mut(ptr, 4);
                            value.marshal(slice);
                            ptr = ptr.add(step);
                        }
                    }
                }
            }
        }
        Column::Timestamp(c) => {
            let mut ptr = ptr;
            match nulls {
                Some((offsize, Some(bitmap))) => {
                    for (value, valid) in c.iter().zip(bitmap.iter()) {
                        unsafe {
                            if valid {
                                let slice = std::slice::from_raw_parts_mut(ptr, 8);
                                value.marshal(slice);
                            } else {
                                ptr.add(offsize).write(1u8);
                            }

                            ptr = ptr.add(step);
                        }
                    }
                }
                _ => {
                    for value in c.iter() {
                        unsafe {
                            let slice = std::slice::from_raw_parts_mut(ptr, 8);
                            value.marshal(slice);
                            ptr = ptr.add(step);
                        }
                    }
                }
            }
        }
        Column::Decimal(c) => {
            with_decimal_mapped_type!(|DECIMAL_TYPE| match c {
                DecimalColumn::DECIMAL_TYPE(t, _) => {
                    let mut ptr = ptr;
                    let count = std::mem::size_of::<DECIMAL_TYPE>();
                    match nulls {
                        Some((offsize, Some(bitmap))) => {
                            for (value, valid) in t.iter().zip(bitmap.iter()) {
                                unsafe {
                                    if valid {
                                        let slice = std::slice::from_raw_parts_mut(ptr, count);
                                        value.marshal(slice);
                                    } else {
                                        ptr.add(offsize).write(1u8);
                                    }

                                    ptr = ptr.add(step);
                                }
                            }
                        }
                        _ => {
                            for value in t.iter() {
                                unsafe {
                                    let slice = std::slice::from_raw_parts_mut(ptr, count);
                                    value.marshal(slice);
                                    ptr = ptr.add(step);
                                }
                            }
                        }
                    }
                }
            })
        }
        _ => {
            return Err(ErrorCode::BadDataValueType(format!(
                "Unsupported apply fn fixed_hash operation for column: {:?}",
                data_type
            )));
        }
    }

    Ok(())
}
