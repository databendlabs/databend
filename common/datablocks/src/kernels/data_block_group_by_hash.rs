// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::DFBinaryArray;
use common_datavalues::DataValue;
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
                            group_values.push(col.try_get(row)?);
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
            let take_block = DataBlock::block_take_by_indices(block, column_names, &group_indices)?;
            group_blocks.push((group_key, group_keys, take_block));
        }

        Ok(group_blocks)
    }

    fn build_keys(&self, group_columns: &[&DataColumn], rows: usize) -> Result<Vec<Self::HashKey>>;
}

pub type HashMethodKeysU8 = HashMethodFixedKeys<UInt8Type>;
pub type HashMethodKeysU16 = HashMethodFixedKeys<UInt16Type>;
pub type HashMethodKeysU32 = HashMethodFixedKeys<UInt32Type>;
pub type HashMethodKeysU64 = HashMethodFixedKeys<UInt64Type>;

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
    pub fn data_type(&self) -> DataType {
        match self {
            HashMethodKind::Serializer(_) => DataType::Binary,
            HashMethodKind::KeysU8(_) => DataType::UInt8,
            HashMethodKind::KeysU16(_) => DataType::UInt16,
            HashMethodKind::KeysU32(_) => DataType::UInt32,
            HashMethodKind::KeysU64(_) => DataType::UInt64,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct HashMethodSerializer {}

impl HashMethodSerializer {
    #[inline]
    pub fn get_key(&self, array: &DFBinaryArray, row: usize) -> Vec<u8> {
        let v = array.as_ref().value(row);
        v.to_owned()
    }

    pub fn de_group_columns(
        &self,
        keys: Vec<Vec<u8>>,
        group_fields: &[DataField],
    ) -> Result<Vec<Series>> {
        let mut keys: Vec<&[u8]> = keys.iter().map(|x| x.as_slice()).collect();
        let rows = keys.len();

        let mut res = Vec::with_capacity(group_fields.len());
        for f in group_fields.iter() {
            let data_type = f.data_type();
            let mut deserializer = data_type.create_deserializer(rows)?;

            for (_row, key) in keys.iter_mut().enumerate() {
                deserializer.de(key)?;
            }
            res.push(deserializer.finish_to_series());
        }
        Ok(res)
    }
}
impl HashMethod for HashMethodSerializer {
    type HashKey = Vec<u8>;

    fn name(&self) -> String {
        "Serializer".to_string()
    }

    fn build_keys(&self, group_columns: &[&DataColumn], rows: usize) -> Result<Vec<Self::HashKey>> {
        let mut group_keys = Vec::with_capacity(rows);
        {
            let mut group_key_len = 0;
            for col in group_columns {
                let typ = col.data_type();
                if common_datavalues::is_integer(&typ) {
                    group_key_len += common_datavalues::numeric_byte_size(&typ)?;
                } else {
                    group_key_len += 4;
                }
            }

            for _i in 0..rows {
                group_keys.push(Vec::with_capacity(group_key_len));
            }

            for col in group_columns {
                DataColumn::serialize(col, &mut group_keys)?;
            }
        }
        Ok(group_keys)
    }
}

pub struct HashMethodFixedKeys<T> {
    t: PhantomData<T>,
}

impl<T> HashMethodFixedKeys<T>
where T: DFNumericType
{
    pub fn default() -> Self {
        HashMethodFixedKeys { t: PhantomData }
    }

    #[inline]
    pub fn get_key(&self, array: &DataArray<T>, row: usize) -> T::Native {
        array.as_ref().value(row)
    }
    pub fn de_group_columns(
        &self,
        keys: Vec<T::Native>,
        group_fields: &[DataField],
    ) -> Result<Vec<Series>> {
        let mut keys = keys;
        let rows = keys.len();
        let step = std::mem::size_of::<T::Native>();
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
        for f in group_fields.iter() {
            let data_type = f.data_type();
            let mut deserializer = data_type.create_deserializer(rows)?;
            let reader = vec8.as_slice();
            deserializer.de_batch(&reader[offsize..], step, rows)?;
            res.push(deserializer.finish_to_series());

            offsize += common_datavalues::numeric_byte_size(data_type)?;
        }
        Ok(res)
    }
}

impl<T> HashMethod for HashMethodFixedKeys<T>
where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Hash + Clone + Debug,
{
    type HashKey = T::Native;

    fn name(&self) -> String {
        format!("FixedKeys{}", std::mem::size_of::<Self::HashKey>())
    }

    fn build_keys(&self, group_columns: &[&DataColumn], rows: usize) -> Result<Vec<Self::HashKey>> {
        let step = std::mem::size_of::<Self::HashKey>();
        let mut group_keys: Vec<u8> = vec![0; rows * step];

        let mut offsize = 0;
        let mut size = step;
        while size > 0 {
            build(
                size,
                &mut offsize,
                group_columns,
                group_keys.as_mut_ptr(),
                step,
            )?;
            size /= 2;
        }

        todo!();
        // Ok(group_keys)
    }
}

#[inline]
fn build(
    mem_size: usize,
    offsize: &mut usize,
    group_columns: &[&DataColumn],
    writer: *mut u8,
    step: usize,
) -> Result<()> {
    for col in group_columns.iter() {
        let data_type = col.data_type();
        let size = common_datavalues::numeric_byte_size(&data_type)?;
        if size == mem_size {
            let series = col.to_array()?;

            let writer = unsafe { writer.add(*offsize) };
            series.group_hash(writer, step)?;
            *offsize += size;
        }
    }
    Ok(())
}
