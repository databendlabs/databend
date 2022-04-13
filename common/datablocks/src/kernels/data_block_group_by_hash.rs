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

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Not;

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::DataBlock;

type GroupIndices<T> = HashMap<T, (Vec<u32>, Vec<DataValue>), ahash::RandomState>;
type GroupBlock<T> = Vec<(T, Vec<DataValue>, DataBlock)>;

pub trait HashMethod {
    type HashKey<'a>: std::cmp::Eq + Hash + Clone + Debug
    where Self: 'a;

    fn name(&self) -> String;
    /// Hash group based on row index then return indices and keys.
    /// For example:
    /// row_idx, A
    /// 0, 1
    /// 1, 2
    /// 2, 3
    /// 3, 4
    /// 4, 5
    ///
    /// grouping by [A%3]
    /// 1)
    /// row_idx, group_key, A
    /// 0, 1, 1
    /// 1, 2, 2
    /// 2, 0, 3
    /// 3, 1, 4
    /// 4, 2, 5
    ///
    /// 2) make indices group(for vector compute)
    /// group_key, indices
    /// 0, [2]
    /// 1, [0, 3]
    /// 2, [1, 4]
    ///

    fn group_by_get_indices<'a>(
        &self,
        block: &'a DataBlock,
        column_names: &[String],
    ) -> Result<GroupIndices<Self::HashKey<'a>>> {
        // Table for <group_key, (indices, keys) >
        let mut group_indices = GroupIndices::<Self::HashKey<'_>>::default();
        // 1. Get group by columns.
        let mut group_columns = Vec::with_capacity(column_names.len());
        {
            for col in column_names {
                group_columns.push(block.try_column_by_name(col)?);
            }
        }

        // 2. Build serialized keys
        let group_keys = self.build_keys(&group_columns, block.num_rows())?;
        // 2. Make group with indices.
        {
            for (row, group_key) in group_keys.iter().enumerate().take(block.num_rows()) {
                match group_indices.get_mut(group_key) {
                    None => {
                        let mut group_values = Vec::with_capacity(group_columns.len());
                        for col in &group_columns {
                            group_values.push(col.get(row));
                        }
                        group_indices.insert(group_key.clone(), (vec![row as u32], group_values));
                    }
                    Some((v, _)) => {
                        v.push(row as u32);
                    }
                }
            }
        }

        Ok(group_indices)
    }

    /// Hash group based on row index by column names.
    ///
    /// group_by_get_indices and make blocks.
    fn group_by<'a>(
        &self,
        block: &'a DataBlock,
        column_names: &[String],
    ) -> Result<GroupBlock<Self::HashKey<'a>>> {
        let group_indices = self.group_by_get_indices(block, column_names)?;
        // Table for <(group_key, keys, block)>
        let mut group_blocks = GroupBlock::<Self::HashKey<'_>>::with_capacity(group_indices.len());

        for (group_key, (group_indices, group_keys)) in group_indices {
            let take_block = DataBlock::block_take_by_indices(block, &group_indices)?;
            group_blocks.push((group_key, group_keys, take_block));
        }

        Ok(group_blocks)
    }

    fn build_keys<'a>(
        &self,
        group_columns: &[&'a ColumnRef],
        rows: usize,
    ) -> Result<Vec<Self::HashKey<'a>>>;
}

pub type HashMethodKeysU8 = HashMethodFixedKeys<u8>;
pub type HashMethodKeysU16 = HashMethodFixedKeys<u16>;
pub type HashMethodKeysU32 = HashMethodFixedKeys<u32>;
pub type HashMethodKeysU64 = HashMethodFixedKeys<u64>;

/// These methods are `generic` method to generate hash key,
/// that is the 'numeric' or 'binary` representation of each column value as hash key.
pub enum HashMethodKind {
    Serializer(HashMethodSerializer),
    SingleString(HashMethodSingleString),
    KeysU8(HashMethodKeysU8),
    KeysU16(HashMethodKeysU16),
    KeysU32(HashMethodKeysU32),
    KeysU64(HashMethodKeysU64),
}

impl HashMethodKind {
    pub fn name(&self) -> String {
        match self {
            HashMethodKind::Serializer(v) => v.name(),
            HashMethodKind::SingleString(v) => v.name(),
            HashMethodKind::KeysU8(v) => v.name(),
            HashMethodKind::KeysU16(v) => v.name(),
            HashMethodKind::KeysU32(v) => v.name(),
            HashMethodKind::KeysU64(v) => v.name(),
        }
    }
    pub fn data_type(&self) -> DataTypePtr {
        match self {
            HashMethodKind::Serializer(_) => Vu8::to_data_type(),
            HashMethodKind::SingleString(_) => Vu8::to_data_type(),
            HashMethodKind::KeysU8(_) => u8::to_data_type(),
            HashMethodKind::KeysU16(_) => u16::to_data_type(),
            HashMethodKind::KeysU32(_) => u32::to_data_type(),
            HashMethodKind::KeysU64(_) => u64::to_data_type(),
        }
    }
}

// A special case for Group by String
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HashMethodSingleString {}

impl HashMethodSingleString {
    #[inline]
    pub fn get_key(&self, column: &StringColumn, row: usize) -> Vec<u8> {
        let v = column.get_data(row);
        v.to_owned()
    }

    pub fn deserialize_group_columns(
        &self,
        keys: Vec<Vec<u8>>,
        group_fields: &[DataField],
    ) -> Result<Vec<ColumnRef>> {
        debug_assert!(!keys.is_empty());
        debug_assert!(group_fields.len() == 1);
        let column = StringColumn::new_from_slice(&keys);
        Ok(vec![column.arc()])
    }
}

impl HashMethod for HashMethodSingleString {
    type HashKey<'a> = &'a [u8];

    fn name(&self) -> String {
        "SingleString".to_string()
    }

    fn build_keys<'a>(
        &self,
        group_columns: &[&'a ColumnRef],
        rows: usize,
    ) -> Result<Vec<&'a [u8]>> {
        debug_assert!(group_columns.len() == 1);
        let column = group_columns[0];
        let str_column: &StringColumn = Series::check_get(column)?;

        let mut values = Vec::with_capacity(rows);
        for row in 0..rows {
            values.push(str_column.get_data(row));
        }
        Ok(values)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct HashMethodSerializer {}

impl HashMethodSerializer {
    #[inline]
    pub fn get_key(&self, column: &StringColumn, row: usize) -> Vec<u8> {
        let v = column.get_data(row);
        v.to_owned()
    }

    pub fn deserialize_group_columns(
        &self,
        keys: Vec<Vec<u8>>,
        group_fields: &[DataField],
    ) -> Result<Vec<ColumnRef>> {
        debug_assert!(!keys.is_empty());
        let mut keys: Vec<&[u8]> = keys.iter().map(|x| x.as_slice()).collect();

        let rows = keys.len();

        let mut res = Vec::with_capacity(group_fields.len());
        for f in group_fields.iter() {
            let data_type = f.data_type();
            let mut deserializer = data_type.create_deserializer(rows);

            for (_row, key) in keys.iter_mut().enumerate() {
                deserializer.de_binary(key)?;
            }
            res.push(deserializer.finish_to_column());
        }
        Ok(res)
    }
}
impl HashMethod for HashMethodSerializer {
    type HashKey<'a> = SmallVu8;

    fn name(&self) -> String {
        "Serializer".to_string()
    }

    fn build_keys(
        &self,
        group_columns: &[&ColumnRef],
        rows: usize,
    ) -> Result<Vec<Self::HashKey<'_>>> {
        let mut group_keys = Vec::with_capacity(rows);
        {
            // TODO: Optimize the SmallVec size by group_key_len

            // let mut group_key_len = 0;
            // for col in group_columns {
            //     let typ = col.data_type();
            //     let typ_id = typ.data_type_id();
            //     if typ_id.is_integer() {
            //         group_key_len += typ_id.numeric_byte_size()?;
            //     } else {
            //         group_key_len += 4;
            //     }
            // }

            for _i in 0..rows {
                group_keys.push(SmallVu8::new());
            }

            for col in group_columns {
                Series::serialize(col, &mut group_keys, None)?
            }
        }
        Ok(group_keys)
    }
}

pub struct HashMethodFixedKeys<T> {
    t: PhantomData<T>,
}

impl<T> HashMethodFixedKeys<T>
where T: PrimitiveType
{
    pub fn default() -> Self {
        HashMethodFixedKeys { t: PhantomData }
    }

    #[inline]
    pub fn get_key(&self, column: &PrimitiveColumn<T>, row: usize) -> T {
        unsafe { column.value_unchecked(row) }
    }
    pub fn deserialize_group_columns(
        &self,
        keys: Vec<T>,
        group_fields: &[DataField],
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

        let mut res = Vec::with_capacity(group_fields.len());
        let mut offsize = 0;

        let mut null_offsize = group_fields
            .iter()
            .map(|c| {
                let ty = c.data_type();
                remove_nullable(ty)
                    .data_type_id()
                    .numeric_byte_size()
                    .unwrap()
            })
            .sum();

        let mut sorted_group_fields = group_fields.to_vec();
        sorted_group_fields.sort_by(|a, b| {
            let a = remove_nullable(a.data_type()).data_type_id();
            let b = remove_nullable(b.data_type()).data_type_id();
            b.numeric_byte_size()
                .unwrap()
                .cmp(&a.numeric_byte_size().unwrap())
        });

        for f in sorted_group_fields.iter() {
            let data_type = f.data_type();
            let non_null_type = remove_nullable(data_type);
            let mut deserializer = non_null_type.create_deserializer(rows);
            let reader = vec8.as_slice();

            let col = match f.is_nullable() {
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
        for f in group_fields.iter() {
            for (sf, col) in sorted_group_fields.iter().zip(res.iter()) {
                if f.data_type() == sf.data_type() && f.name() == sf.name() {
                    result_columns.push(col.clone());
                    break;
                }
            }
        }
        Ok(result_columns)
    }
}

impl<T> HashMethod for HashMethodFixedKeys<T>
where
    T: PrimitiveType,
    T: std::cmp::Eq + Hash + Clone + Debug,
{
    type HashKey<'a> = T;

    fn name(&self) -> String {
        format!("FixedKeys{}", std::mem::size_of::<Self::HashKey<'_>>())
    }

    // More details about how it works, see: Series::fixed_hash
    fn build_keys(
        &self,
        group_columns: &[&ColumnRef],
        rows: usize,
    ) -> Result<Vec<Self::HashKey<'_>>> {
        let step = std::mem::size_of::<T>();
        let mut group_keys: Vec<T> = vec![T::default(); rows];
        let ptr = group_keys.as_mut_ptr() as *mut u8;
        let mut offsize = 0;
        let mut null_offsize = group_columns
            .iter()
            .map(|c| {
                let ty = c.data_type();
                remove_nullable(&ty)
                    .data_type_id()
                    .numeric_byte_size()
                    .unwrap()
            })
            .sum();

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
