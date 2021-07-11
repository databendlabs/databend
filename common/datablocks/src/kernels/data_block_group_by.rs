// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use common_datavalues::prelude::*;
use common_datavalues::DFBinaryArray;
use common_datavalues::DFUInt32Array;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::DataBlock;

pub trait HashMethod {
    type HashKey: std::cmp::Eq + Hash + Clone + Debug;

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
    ) -> Result<HashMap<Self::HashKey, (Vec<u32>, Vec<DataValue>), ahash::RandomState>> {
        // Table for <group_key, (indices, keys) >
        let mut group_indices =
            HashMap::<Self::HashKey, (Vec<u32>, Vec<DataValue>), ahash::RandomState>::default();
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
    ) -> Result<Vec<(Self::HashKey, Vec<DataValue>, DataBlock)>> {
        let group_indices = self.group_by_get_indices(block, column_names)?;
        // Table for <(group_key, keys, block)>
        let mut group_blocks =
            Vec::<(Self::HashKey, Vec<DataValue>, DataBlock)>::with_capacity(group_indices.len());

        for (group_key, (group_indices, group_keys)) in group_indices {
            let take_block = DataBlock::block_take_by_indices(block, column_names, &group_indices)?;
            group_blocks.push((group_key, group_keys, take_block));
        }

        Ok(group_blocks)
    }

    fn build_keys(&self, group_columns: &[&DataColumn], rows: usize) -> Result<Vec<Self::HashKey>>;
}

#[derive(Debug, Clone)]
pub struct HashMethodSerializer {}

impl HashMethodSerializer {
    pub fn new() -> Self {
        Self {}
    }

    #[inline]
    pub fn get_key(&self, array: &DFBinaryArray, row: usize) -> Vec<u8> {
        let v = array.as_ref().value(row);
        v.to_owned()
    }
}
impl HashMethod for HashMethodSerializer {
    type HashKey = Vec<u8>;

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

#[derive(Debug, Clone)]

pub struct HashMethodKeysU32 {}
impl HashMethodKeysU32 {
    pub fn new() -> Self {
        Self {}
    }

    #[inline]
    pub fn get_key(&self, array: &DFUInt32Array, row: usize) -> u32 {
        array.as_ref().value(row)
    }
}
impl HashMethod for HashMethodKeysU32 {
    type HashKey = u32;

    fn build_keys(&self, group_columns: &[&DataColumn], rows: usize) -> Result<Vec<Self::HashKey>> {
        fn build_u32(
            offsize: &mut usize,
            group_columns: &[&DataColumn],
            ptr: *mut u8,
            step: usize,
        ) -> Result<()> {
            for col in group_columns.iter() {
                let data_type = col.data_type();
                let size = common_datavalues::numeric_byte_size(&data_type)?;
                if size == 4 {
                    let start_ptr = ptr as usize + *offsize;
                    let series = col.to_array()?;
                    series.group_hash(start_ptr, step)?;

                    *offsize = *offsize + size;
                }
            }
            Ok(())
        }

        fn build_u16(
            offsize: &mut usize,
            group_columns: &[&DataColumn],
            ptr: *mut u8,
            step: usize,
        ) -> Result<()> {
            for col in group_columns.iter() {
                let data_type = col.data_type();
                let size = common_datavalues::numeric_byte_size(&data_type)?;
                if size == 2 {
                    let start_ptr = ptr as usize + *offsize;
                    let series = col.to_array()?;
                    series.group_hash(start_ptr, step)?;

                    *offsize = *offsize + size;
                }
            }
            Ok(())
        }

        fn build_u8(
            offsize: &mut usize,
            group_columns: &[&DataColumn],
            ptr: *mut u8,
            step: usize,
        ) -> Result<()> {
            for col in group_columns.iter() {
                let data_type = col.data_type();
                let size = common_datavalues::numeric_byte_size(&data_type)?;
                if size == 1 {
                    let start_ptr = ptr as usize + *offsize;
                    let series = col.to_array()?;
                    series.group_hash(start_ptr, step)?;
                    *offsize = *offsize + size;
                }
            }
            Ok(())
        }

        let mut group_keys = vec![0; rows];
        let mut offsize = 0;
        let ptr = group_keys.as_mut_ptr() as *mut u8;
        let step = std::mem::size_of::<u32>();

        build_u32(&mut offsize, group_columns, ptr, step)?;
        build_u16(&mut offsize, group_columns, ptr, step)?;
        build_u8(&mut offsize, group_columns, ptr, step)?;

        Ok(group_keys)
    }
}

#[derive(Debug, Clone)]
pub enum HashMethodKind {
    Serializer(HashMethodSerializer),
    KeysU32(HashMethodKeysU32),
}

impl DataBlock {
    pub fn choose_hash_method(
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<HashMethodKind> {
        let mut group_key_len = 0;
        for col in column_names {
            let column = block.try_column_by_name(col)?;
            let typ = column.data_type();
            if common_datavalues::is_integer(&typ) {
                group_key_len += common_datavalues::numeric_byte_size(&typ)?;
            } else {
                return Ok(HashMethodKind::Serializer(HashMethodSerializer::new()));
            }
        }
        match group_key_len {
            0..=32 => Ok(HashMethodKind::KeysU32(HashMethodKeysU32::new())),
            _ => Ok(HashMethodKind::Serializer(HashMethodSerializer::new())),
        }
    }
}
