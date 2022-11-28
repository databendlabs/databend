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

use common_datavalues::prelude::*;
use common_exception::Result;
use primitive_types::U256;
use primitive_types::U512;

pub enum KeysState {
    Column(ColumnRef),
    U128(Vec<u128>),
    U256(Vec<U256>),
    U512(Vec<U512>),
}

pub trait HashMethod: Clone {
    type HashKey: ?Sized + Eq + Hash + Debug;

    type HashKeyIter<'a>: Iterator<Item = &'a Self::HashKey> + TrustedLen
    where Self: 'a;

    fn name(&self) -> String;

    fn build_keys_state(&self, group_columns: &[&ColumnRef], rows: usize) -> Result<KeysState>;

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
#[derive(Clone)]
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
    pub fn data_type(&self) -> DataTypeImpl {
        match self {
            HashMethodKind::Serializer(_) => Vu8::to_data_type(),
            HashMethodKind::KeysU8(_) => u8::to_data_type(),
            HashMethodKind::KeysU16(_) => u16::to_data_type(),
            HashMethodKind::KeysU32(_) => u32::to_data_type(),
            HashMethodKind::KeysU64(_) => u64::to_data_type(),
            HashMethodKind::KeysU128(_) => Vu8::to_data_type(),
            HashMethodKind::KeysU256(_) => Vu8::to_data_type(),
            HashMethodKind::KeysU512(_) => Vu8::to_data_type(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HashMethodSerializer {}

impl HashMethod for HashMethodSerializer {
    type HashKey = [u8];

    type HashKeyIter<'a> = StringValueIter<'a>;

    fn name(&self) -> String {
        "Serializer".to_string()
    }

    fn build_keys_state(&self, group_columns: &[&ColumnRef], rows: usize) -> Result<KeysState> {
        if group_columns.len() == 1 && group_columns[0].data_type_id() == TypeID::String {
            return Ok(KeysState::Column(group_columns[0].convert_full_column()));
        }

        let approx_size = group_columns.len() * rows * 8;
        let mut values = Vec::with_capacity(approx_size);
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0i64);

        for row in 0..rows {
            for col in group_columns {
                col.serialize(&mut values, row);
            }
            offsets.push(values.len() as i64);
        }
        let col = unsafe { StringColumn::from_data_unchecked(offsets.into(), values.into()) };
        Ok(KeysState::Column(col.arc()))
    }

    fn build_keys_iter<'a>(&self, key_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match key_state {
            KeysState::Column(col) => {
                // maybe constant column
                let str_column: &StringColumn = Series::check_get(col)?;
                Ok(str_column.iter())
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
where T: PrimitiveType
{
    #[inline]
    pub fn get_key(&self, column: &PrimitiveColumn<T>, row: usize) -> T {
        unsafe { column.value_unchecked(row) }
    }
}

impl<T> HashMethodFixedKeys<T>
where T: Clone + Default
{
    fn build_keys_vec<'a>(&self, group_columns: &[&'a ColumnRef], rows: usize) -> Result<Vec<T>> {
        let step = std::mem::size_of::<T>();
        let mut group_keys: Vec<T> = vec![T::default(); rows];
        let ptr = group_keys.as_mut_ptr() as *mut u8;
        let mut offsize = 0;
        let mut null_offsize = group_columns
            .iter()
            .map(|c| {
                let ty = c.data_type();
                remove_nullable(&ty).data_type_id().numeric_byte_size()
            })
            .sum::<Result<usize>>()?;

        let mut group_columns = group_columns.to_vec();
        group_columns.sort_by(|a, b| {
            let a = remove_nullable(&a.data_type()).data_type_id();
            let b = remove_nullable(&b.data_type()).data_type_id();
            b.numeric_byte_size()
                .unwrap()
                .cmp(&a.numeric_byte_size().unwrap())
        });

        for col in group_columns.iter() {
            build(&mut offsize, &mut null_offsize, col, ptr, step)?;
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
        group_items: &[(usize, DataTypeImpl)],
    ) -> Result<Vec<ColumnRef>> {
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

        let mut res = Vec::with_capacity(group_items.len());
        let mut offsize = 0;

        let mut null_offsize = group_items
            .iter()
            .map(|(_, ty)| remove_nullable(ty).data_type_id().numeric_byte_size())
            .sum::<Result<usize>>()?;

        let mut sorted_group_items = group_items.to_vec();
        sorted_group_items.sort_by(|(_, a), (_, b)| {
            let a = remove_nullable(a).data_type_id();
            let b = remove_nullable(b).data_type_id();
            b.numeric_byte_size()
                .unwrap()
                .cmp(&a.numeric_byte_size().unwrap())
        });

        for (_, data_type) in sorted_group_items.iter() {
            let non_null_type = remove_nullable(data_type);
            let mut deserializer = non_null_type.create_deserializer(rows);
            let reader = vec8.as_slice();

            let col = match data_type.is_nullable() {
                false => {
                    deserializer.de_fixed_binary_batch(&reader[offsize..], step, rows)?;
                    deserializer.finish_to_column()
                }

                true => {
                    let mut bitmap_deserializer = bool::to_data_type().create_deserializer(rows);
                    bitmap_deserializer.de_fixed_binary_batch(
                        &reader[null_offsize..],
                        step,
                        rows,
                    )?;

                    null_offsize += 1;

                    let col = bitmap_deserializer.finish_to_column();
                    let col: &BooleanColumn = Series::check_get(&col)?;

                    // we store 1 for nulls in fixed_hash
                    let bitmap = col.values().not();
                    deserializer.de_fixed_binary_batch(&reader[offsize..], step, rows)?;
                    let inner = deserializer.finish_to_column();
                    NullableColumn::wrap_inner(inner, Some(bitmap))
                }
            };

            offsize += non_null_type.data_type_id().numeric_byte_size()?;
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
    ($ty:ty) => {
        impl HashMethod for HashMethodFixedKeys<$ty> {
            type HashKey = $ty;
            type HashKeyIter<'a> = std::slice::Iter<'a, $ty>;

            fn name(&self) -> String {
                format!("FixedKeys{}", std::mem::size_of::<Self::HashKey>())
            }

            fn build_keys_state(
                &self,
                group_columns: &[&ColumnRef],
                rows: usize,
            ) -> Result<KeysState> {
                // faster path for single fixed keys
                if group_columns.len() == 1
                    && matches!(
                        group_columns[0].data_type_id(),
                        TypeID::UInt8 | TypeID::UInt16 | TypeID::UInt32 | TypeID::UInt64
                    )
                {
                    return Ok(KeysState::Column(group_columns[0].convert_full_column()));
                }
                let keys = self.build_keys_vec(group_columns, rows)?;
                let col = PrimitiveColumn::<$ty>::from_vecs(keys);
                Ok(KeysState::Column(col.arc()))
            }

            fn build_keys_iter<'a>(
                &self,
                key_state: &'a KeysState,
            ) -> Result<Self::HashKeyIter<'a>> {
                match key_state {
                    KeysState::Column(col) => {
                        let col: &PrimitiveColumn<$ty> = Series::check_get(col)?;
                        Ok(col.iter())
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
                group_columns: &[&ColumnRef],
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
    col: &ColumnRef,
    writer: *mut u8,
    step: usize,
) -> Result<()> {
    let data_type_nonull = remove_nullable(&col.data_type());
    let size = data_type_nonull.data_type_id().numeric_byte_size()?;

    let writer = unsafe { writer.add(*offsize) };
    let nulls = if col.is_nullable() {
        // origin_ptr-------ptr<------old------>null_offset
        let null_offsize_from_ptr = *null_offsize - *offsize;
        *null_offsize += 1;
        Some((null_offsize_from_ptr, None))
    } else {
        None
    };

    Series::fixed_hash(col, writer, step, nulls)?;
    *offsize += size;
    Ok(())
}
