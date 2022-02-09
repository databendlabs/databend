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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use crate::prelude::*;

impl Series {
    pub fn fixed_hash(column: &ColumnRef, ptr: *mut u8, step: usize) -> Result<()> {
        let column = column.convert_full_column();
        // TODO support nullable
        let column = Series::remove_nullable(&column);
        let type_id = column.data_type_id().to_physical_type();

        with_match_scalar_type!(type_id, |$T| {
            let col: &<$T as Scalar>::ColumnType = Series::check_get(&column)?;
            GroupHash::fixed_hash(col, ptr, step)
        }, {
            Err(ErrorCode::BadDataValueType(
                format!("Unsupported apply fn fixed_hash operation for column: {:?}", column.data_type()),
            ))
        })
    }

    /// Apply binary mode function to each element of the column.
    /// WARN: Can't use `&mut [Vec<u8>]` because it has performance drawback.
    /// Refer: https://github.com/rust-lang/rust-clippy/issues/8334
    pub fn serialize(column: &ColumnRef, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        let column = column.convert_full_column();
        // TODO support nullable
        let column = Series::remove_nullable(&column);
        let type_id = column.data_type_id().to_physical_type();

        with_match_scalar_type!(type_id, |$T| {
            let col: &<$T as Scalar>::ColumnType = Series::check_get(&column)?;
            GroupHash::serialize(col, vec)
        }, {
             Err(ErrorCode::BadDataValueType(
                format!("Unsupported apply fn serialize operation for column: {:?}", column.data_type()),
            ))
        })
    }
}

// Read more:
//  https://www.cockroachlabs.com/blog/vectorized-hash-joiner/
//  http://myeyesareblind.com/2017/02/06/Combine-hash-values/

pub trait GroupHash: Debug {
    /// Compute the hash for all values in the array.
    fn fixed_hash(&self, _ptr: *mut u8, _step: usize) -> Result<()> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply fn fixed_hash operation for {:?}",
            self,
        )))
    }

    fn serialize(&self, _vec: &mut Vec<Vec<u8>>) -> Result<()> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply fn serialize operation for {:?}",
            self,
        )))
    }
}

impl<T> GroupHash for PrimitiveColumn<T>
where
    T: PrimitiveType,
    T: Marshal + StatBuffer + Sized,
{
    fn fixed_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let mut ptr = ptr;
        // TODO: (sundy) we use reinterpret_cast here, it gains much performance
        for value in self.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    value as *const T as *const u8,
                    ptr,
                    std::mem::size_of::<T>(),
                );
                ptr = ptr.add(step);
            }
        }
        Ok(())
    }

    fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        assert_eq!(vec.len(), self.len());
        for (value, vec) in self.iter().zip(vec.iter_mut()) {
            BinaryWrite::write_scalar(vec, value)?;
        }
        Ok(())
    }
}

impl GroupHash for BooleanColumn {
    fn fixed_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let mut ptr = ptr;

        for value in self.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(&(value as u8) as *const u8, ptr, 1);
                ptr = ptr.add(step);
            }
        }
        Ok(())
    }

    fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        assert_eq!(vec.len(), self.len());
        for (value, vec) in self.iter().zip(vec.iter_mut()) {
            BinaryWrite::write_scalar(vec, &value)?;
        }
        Ok(())
    }
}

impl GroupHash for StringColumn {
    fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        assert_eq!(vec.len(), self.len());
        for (value, vec) in self.iter().zip(vec.iter_mut()) {
            BinaryWrite::write_binary(vec, value)?;
        }
        Ok(())
    }
}
