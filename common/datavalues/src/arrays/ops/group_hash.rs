// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use std::fmt::Debug;

use arrays::DataArray;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use crate::*;

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

impl<T> GroupHash for DataArray<T>
where
    T: DFNumericType,
    T::Native: Marshal + StatBuffer + Sized,
{
    fn fixed_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        let mut buffer = T::Native::buffer();
        for value in array.values().iter() {
            value.marshal(buffer.as_mut());
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &buffer.as_ref()[0] as *const u8,
                    ptr,
                    std::mem::size_of::<T::Native>(),
                );
                ptr = ptr.add(step);
            }
        }
        Ok(())
    }

    fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        assert_eq!(vec.len(), self.len());
        for (value, vec) in self.into_no_null_iter().zip(vec.iter_mut()) {
            BinaryWrite::write_scalar(vec, &value)?;
        }
        Ok(())
    }
}

impl GroupHash for DFBooleanArray {
    fn fixed_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        let mut buffer = bool::buffer();
        for value in array.values().iter() {
            value.marshal(buffer.as_mut());
            unsafe {
                std::ptr::copy_nonoverlapping(&buffer.as_ref()[0] as *const u8, ptr, 1);
                ptr = ptr.add(step);
            }
        }
        Ok(())
    }

    fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        assert_eq!(vec.len(), self.len());
        for (value, vec) in self.into_no_null_iter().zip(vec.iter_mut()) {
            BinaryWrite::write_scalar(vec, &value)?;
        }
        Ok(())
    }
}

impl GroupHash for DFUtf8Array {
    fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        assert_eq!(vec.len(), self.len());
        for (value, vec) in self.into_no_null_iter().zip(vec.iter_mut()) {
            BinaryWrite::write_string(vec, value)?;
        }
        Ok(())
    }
}

impl GroupHash for DFListArray {}
impl GroupHash for DFBinaryArray {}
impl GroupHash for DFNullArray {}
impl GroupHash for DFStructArray {}
