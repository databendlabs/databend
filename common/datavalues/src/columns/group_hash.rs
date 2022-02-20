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
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use crate::prelude::*;

impl Series {
    /// Hash function for nullable column, which directly operate on data to avoid invocation of virtual function.
    pub fn do_hash_for_nullable_column(
        ptr: *mut u8,
        column: &Arc<dyn Column>,
        bitmap: &Bitmap,
        step: usize,
        null_offset: usize,
    ) -> Result<()> {
        Series::fixed_hash(column, ptr, step)?;
        for (row, valid_row) in bitmap.iter().enumerate() {
            if !valid_row {
                unsafe {
                    ptr.add(row * step + null_offset).write(0x01);
                }
            }
        }
        Ok(())
    }

    pub fn fixed_hash(column: &ColumnRef, ptr: *mut u8, step: usize) -> Result<()> {
        let column = column.convert_full_column();
        // TODO support nullable
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

    pub fn fixed_hash_with_nullable(
        column: &ColumnRef,
        ptr: *mut u8,
        step: usize,
        null_offset: usize,
    ) -> Result<()> {
        let column = column.convert_full_column();
        // TODO support nullable
        // let column = Series::restore_nullable_if_nullable(&column);
        let type_id = (remove_nullable(&column.data_type()).data_type_id()).to_physical_type();
        with_match_scalar_type!(type_id, |$T|
        {
            let col: &NullableColumn = Series::check_get(&column)?;
            GroupHash::fixed_hash_with_nullable(col, ptr, step, null_offset)
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
    fn fixed_hash_with_nullable(
        &self,
        _ptr: *mut u8,
        _step: usize,
        _null_offset: usize,
    ) -> Result<()> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply fn nullable_hash operation for {:?}",
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

impl GroupHash for NullableColumn {
    fn fixed_hash_with_nullable(
        &self,
        ptr: *mut u8,
        step: usize,
        null_offset: usize,
    ) -> Result<()> {
        let bitmap = self.ensure_validity();
        let inner_column = self.inner();
        Series::do_hash_for_nullable_column(ptr, inner_column, bitmap, step, null_offset)?;
        Ok(())
    }

    fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        assert_eq!(vec.len(), self.inner().len());
        Series::serialize(self.inner(), vec)?;
        Ok(())
    }

    fn fixed_hash(&self, _ptr: *mut u8, _step: usize) -> Result<()> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply fn fixed_hash operation for {:?}",
            self,
        )))
    }
}
