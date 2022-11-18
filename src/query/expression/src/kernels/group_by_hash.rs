// Copyright 2021 Datafuse Labs.
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
use std::hash::Hash;
use std::iter::TrustedLen;
use std::marker::PhantomData;
use std::ops::Not;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use primitive_types::U256;
use primitive_types::U512;

use crate::types::number::Number;
use crate::types::string::StringIterator;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::NumberDataType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::Column;
use crate::DataField;
use crate::SchemaDataType;
use crate::Value;

pub enum KeysState {
    Column(Column),
    U128(Vec<u128>),
    U256(Vec<U256>),
    U512(Vec<U512>),
}

pub trait HashMethod: Clone {
    type HashKey: ?Sized + Eq + Hash + Debug;

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
pub type HashMethodKeysU256 = HashMethodFixedKeys<U256>;
pub type HashMethodKeysU512 = HashMethodFixedKeys<U512>;

/// These methods are `generic` method to generate hash key,
/// that is the 'numeric' or 'binary` representation of each column value as hash key.
pub enum HashMethodKind {
    Serializer(HashMethodSerializer),
    KeysU8(HashMethodKeysU8),
    KeysU16(HashMethodKeysU16),
    KeysU32(HashMethodKeysU32),
    KeysU64(HashMethodKeysU64),
    KeysU128(HashMethodKeysU128),
    KeysU256(HashMethodKeysU256),
    KeysU512(HashMethodKeysU512),
}

impl HashMethodKind {
    pub fn name(&self) -> String {
        match self {
            HashMethodKind::Serializer(v) => v.name(),
            HashMethodKind::KeysU8(v) => v.name(),
            HashMethodKind::KeysU16(v) => v.name(),
            HashMethodKind::KeysU32(v) => v.name(),
            HashMethodKind::KeysU64(v) => v.name(),
            HashMethodKind::KeysU128(v) => v.name(),
            HashMethodKind::KeysU256(v) => v.name(),
            HashMethodKind::KeysU512(v) => v.name(),
        }
    }
    pub fn data_type(&self) -> DataType {
        match self {
            HashMethodKind::Serializer(_) => DataType::String,
            HashMethodKind::KeysU8(_) => DataType::Number(NumberDataType::UInt8),
            HashMethodKind::KeysU16(_) => DataType::Number(NumberDataType::UInt16),
            HashMethodKind::KeysU32(_) => DataType::Number(NumberDataType::UInt32),
            HashMethodKind::KeysU64(_) => DataType::Number(NumberDataType::UInt64),
            HashMethodKind::KeysU128(_)
            | HashMethodKind::KeysU256(_)
            | HashMethodKind::KeysU512(_) => DataType::String,
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
        if group_columns.len() == 1 && group_columns[0].1 == DataType::String {
            return Ok(KeysState::Column(group_columns[0].0.clone()));
        }

        todo!("")
    }

    fn build_keys_iter<'a>(&self, key_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match key_state {
            KeysState::Column(col) => {
                // maybe constant column
                // let str_column = StringType::try_downcast_column(col).unwrap();
                // Ok(str_column.iter())
                todo!("expression")
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Clone)]
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
    fn build_keys_vec<'a>(
        &self,
        group_columns: &[(Column, DataType)],
        rows: usize,
    ) -> Result<Vec<T>> {
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

        todo!("expression");
    }
}

macro_rules! impl_hash_method_fixed_keys {
    ($ty:ty) => {
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
                if group_columns.len() == 1 && group_columns[0].1.is_unsigned_numeric() {
                    return Ok(KeysState::Column(group_columns[0].0.clone()));
                }
                let keys = self.build_keys_vec(group_columns, rows)?;
                let col = Buffer::<$ty>::from(keys);
                Ok(KeysState::Column(NumberType::<$ty>::upcast_column(col)))
            }

            fn build_keys_iter<'a>(
                &self,
                key_state: &'a KeysState,
            ) -> Result<Self::HashKeyIter<'a>> {
                match key_state {
                    KeysState::Column(col) => {
                        // let col = NumberType::<$ty>::try_downcast_column(col).unwrap();
                        // Ok(col.iter())
                        todo!("expression")
                    }
                    _ => unreachable!(),
                }
            }
        }
    };
}

impl_hash_method_fixed_keys! {u8}
impl_hash_method_fixed_keys! {u16}
impl_hash_method_fixed_keys! {u32}
impl_hash_method_fixed_keys! {u64}

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
                let keys = self.build_keys_vec(group_columns, rows)?;
                Ok(KeysState::$name(keys))
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
impl_hash_method_fixed_large_keys! {U512, U512}

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

    todo!("expression");
    // Series::fixed_hash(col, writer, step, nulls)?;
    *offsize += size;
    Ok(())
}
