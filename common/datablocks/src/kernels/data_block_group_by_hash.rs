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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::DataBlock;

type GroupIndices<T> = HashMap<T, (Vec<u32>, Vec<DataValue>), ahash::RandomState>;
type GroupBlock<T> = Vec<(T, Vec<DataValue>, DataBlock)>;

pub trait HashMethod {
    type HashKey: std::cmp::Eq + Hash + Clone + Debug;

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

    fn group_by_get_indices(
        &self,
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<GroupIndices<Self::HashKey>> {
        // Table for <group_key, (indices, keys) >
        let mut group_indices = GroupIndices::<Self::HashKey>::default();
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
    fn group_by(
        &self,
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<GroupBlock<Self::HashKey>> {
        let group_indices = self.group_by_get_indices(block, column_names)?;
        // Table for <(group_key, keys, block)>
        let mut group_blocks = GroupBlock::<Self::HashKey>::with_capacity(group_indices.len());

        for (group_key, (group_indices, group_keys)) in group_indices {
            let take_block = DataBlock::block_take_by_indices(block, &group_indices)?;
            group_blocks.push((group_key, group_keys, take_block));
        }

        Ok(group_blocks)
    }

    fn build_keys(&self, group_columns: &[&ColumnRef], rows: usize) -> Result<Vec<Self::HashKey>>;
}

pub type HashMethodKeysU8 = HashMethodFixedKeys<u8>;
pub type HashMethodKeysU16 = HashMethodFixedKeys<u16>;
pub type HashMethodKeysU32 = HashMethodFixedKeys<u32>;
pub type HashMethodKeysU64 = HashMethodFixedKeys<u64>;

/// These methods are `generic` method to generate hash key,
/// that is the 'binary` representation of each column value as hash key.
pub enum HashMethodKind {
    Serializer(HashMethodSerializer),
    KeysU8(HashMethodKeysU8),
    KeysU16(HashMethodKeysU16),
    KeysU32(HashMethodKeysU32),
    KeysU64(HashMethodKeysU64),
}

impl HashMethodKind {
    pub fn name(&self) -> String {
        match self {
            HashMethodKind::Serializer(v) => v.name(),
            HashMethodKind::KeysU8(v) => v.name(),
            HashMethodKind::KeysU16(v) => v.name(),
            HashMethodKind::KeysU32(v) => v.name(),
            HashMethodKind::KeysU64(v) => v.name(),
        }
    }
    pub fn data_type(&self) -> DataTypePtr {
        match self {
            HashMethodKind::Serializer(_) => Vu8::to_data_type(),
            HashMethodKind::KeysU8(_) => u8::to_data_type(),
            HashMethodKind::KeysU16(_) => u16::to_data_type(),
            HashMethodKind::KeysU32(_) => u32::to_data_type(),
            HashMethodKind::KeysU64(_) => u64::to_data_type(),
        }
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

    pub fn de_group_columns(
        &self,
        keys: Vec<Vec<u8>>,
        group_fields: &[DataField],
    ) -> Result<Vec<ColumnRef>> {
        let mut keys: Vec<&[u8]> = keys.iter().map(|x| x.as_slice()).collect();
        let rows = keys.len();

        let mut res = Vec::with_capacity(group_fields.len());
        for f in group_fields.iter() {
            let data_type = f.data_type();
            let mut deserializer = data_type.create_deserializer(rows);

            for (_row, key) in keys.iter_mut().enumerate() {
                deserializer.de(key)?;
            }
            res.push(deserializer.finish_to_column());
        }
        Ok(res)
    }
}
impl HashMethod for HashMethodSerializer {
    type HashKey = Vec<u8>;

    fn name(&self) -> String {
        "Serializer".to_string()
    }

    fn build_keys(&self, group_columns: &[&ColumnRef], rows: usize) -> Result<Vec<Self::HashKey>> {
        let mut group_keys = Vec::with_capacity(rows);
        {
            let mut group_key_len = 0;
            for col in group_columns {
                let typ = col.data_type();
                let typ_id = typ.data_type_id();
                if typ_id.is_integer() {
                    group_key_len += typ_id.numeric_byte_size()?;
                } else {
                    group_key_len += 4;
                }
            }

            for _i in 0..rows {
                group_keys.push(Vec::with_capacity(group_key_len));
            }

            for col in group_columns {
                Series::serialize(col, &mut group_keys)?
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
    // todo() de_group_columns supports nullable().
    pub fn de_group_columns(
        &self,
        keys: Vec<T>,
        group_fields: &[DataField],
    ) -> Result<Vec<ColumnRef>> {
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
        let mut null_part_offset = 0;

        init_nullable_offset_via_fields(&mut null_part_offset, group_fields)?;
        for f in group_fields.iter() {
            let data_type = f.data_type();
            let mut deserializer = data_type.create_deserializer(rows);
            let reader = vec8.as_slice();
            if !data_type.is_nullable() {
                deserializer.de_batch(&reader[offsize..], step, rows)?;
                res.push(deserializer.finish_to_column());
            } else {
                deserializer.de_batch_with_nullable(
                    &reader[offsize..],
                    step,
                    rows,
                    null_part_offset,
                )?;
                res.push(deserializer.finish_to_column());
                null_part_offset += 1;
            }
            if data_type.is_nullable() {
                offsize += remove_nullable(data_type)
                    .data_type_id()
                    .numeric_byte_size()?;
            } else {
                offsize += data_type.data_type_id().numeric_byte_size()?;
            }
        }
        Ok(res)
    }
}

impl<T> HashMethod for HashMethodFixedKeys<T>
where
    T: PrimitiveType,
    T: std::cmp::Eq + Hash + Clone + Debug,
{
    type HashKey = T;

    fn name(&self) -> String {
        format!("FixedKeys{}", std::mem::size_of::<Self::HashKey>())
    }

    fn build_keys(&self, group_columns: &[&ColumnRef], rows: usize) -> Result<Vec<Self::HashKey>> {
        let step = std::mem::size_of::<T>();
        // Use group_keys to store group keys.
        let mut group_keys: Vec<T> = vec![T::default(); rows];
        let ptr = group_keys.as_mut_ptr() as *mut u8;
        let mut offsize = 0;
        let mut size = step;

        let group_columns_has_nullable_one = check_group_columns_has_nullable(group_columns);
        if group_columns_has_nullable_one {
            let mut null_part_offset = 0;
            init_nullable_offset(&mut null_part_offset, group_columns)?;
            while size > 0 {
                build_keys_with_nullable_column(
                    size,
                    &mut offsize,
                    group_columns,
                    ptr,
                    step,
                    &mut null_part_offset,
                )?;
                size /= 2;
            }
        } else {
            while size > 0 {
                build(size, &mut offsize, group_columns, ptr, step)?;
                size /= 2;
            }
        }
        Ok(group_keys)
    }

    fn group_by_get_indices(
        &self,
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<GroupIndices<Self::HashKey>> {
        // Table for <group_key, (indices, keys) >
        let mut group_indices = GroupIndices::<Self::HashKey>::default();
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
                        group_indices
                            .insert((*group_key).clone(), (vec![row as u32], group_values));
                    }
                    Some((v, _)) => {
                        v.push(row as u32);
                    }
                }
            }
        }

        Ok(group_indices)
    }

    fn group_by(
        &self,
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<GroupBlock<Self::HashKey>> {
        let group_indices = self.group_by_get_indices(block, column_names)?;
        // Table for <(group_key, keys, block)>
        let mut group_blocks = GroupBlock::<Self::HashKey>::with_capacity(group_indices.len());

        for (group_key, (group_indices, group_keys)) in group_indices {
            let take_block = DataBlock::block_take_by_indices(block, &group_indices)?;
            group_blocks.push((group_key, group_keys, take_block));
        }

        Ok(group_blocks)
    }
}

/// Init the nullable part's offset in bytes. It follows the values part.
#[inline]
fn init_nullable_offset(null_part_offset: &mut usize, group_keys: &[&ColumnRef]) -> Result<()> {
    for group_key_column in group_keys {
        *null_part_offset += remove_nullable(&group_key_column.data_type())
            .data_type_id()
            .numeric_byte_size()?;
    }
    Ok(())
}

/// Init the nullable part's offset in bytes. It follows the values part.
#[inline]
fn init_nullable_offset_via_fields(
    null_part_offset: &mut usize,
    group_fields: &[DataField],
) -> Result<()> {
    for f in group_fields {
        let f_typ = f.data_type();
        if f_typ.is_nullable() {
            *null_part_offset += remove_nullable(f_typ).data_type_id().numeric_byte_size()?;
        } else {
            *null_part_offset += f_typ.data_type_id().numeric_byte_size()?;
        }
    }
    Ok(())
}

#[inline]
fn build(
    mem_size: usize,
    offsize: &mut usize,
    group_columns: &[&ColumnRef],
    writer: *mut u8,
    step: usize,
) -> Result<()> {
    for col in group_columns.iter() {
        let data_type = col.data_type();
        let type_id = data_type.data_type_id();
        let size = type_id.numeric_byte_size()?;
        if size == mem_size {
            let writer = unsafe { writer.add(*offsize) };
            Series::fixed_hash(col, writer, step)?;
            *offsize += size;
        }
    }
    Ok(())
}

#[inline]
fn build_keys_with_nullable_column(
    mem_size: usize,
    offsize: &mut usize,
    group_columns: &[&ColumnRef],
    writer: *mut u8,
    step: usize,
    null_offset: &mut usize,
) -> Result<()> {
    for col in group_columns.iter() {
        let data_type = col.data_type();
        let type_id = remove_nullable(&data_type).data_type_id();
        let size = type_id.numeric_byte_size()?;

        if size == mem_size {
            if col.is_nullable() {
                let writer = unsafe { writer.add(*offsize) };
                Series::fixed_hash_with_nullable(col, writer, step, *null_offset)?;
                *offsize += size;
                *null_offset += 1;
            } else {
                let writer = unsafe { writer.add(*offsize) };
                Series::fixed_hash(col, writer, step)?;
                *offsize += size;
            }
        }
    }
    Ok(())
}

#[inline]
fn check_group_columns_has_nullable(group_columns: &[&ColumnRef]) -> bool {
    for group_column in group_columns.iter() {
        if group_column.is_nullable() {
            return true;
        }
    }
    false
}
