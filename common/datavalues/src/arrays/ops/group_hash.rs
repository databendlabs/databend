// Copyright 2020 Datafuse Labs.
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

impl<T> GroupHash for DFPrimitiveArray<T>
where
    T: DFPrimitiveType,
    T: Marshal + StatBuffer + Sized,
{
    fn fixed_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.inner();
        let mut ptr = ptr;
        // let mut buffer = T::buffer();
        // value.marshal(buffer.as_mut());
        // &buffer.as_ref()[0] as *const u8,

        // TODO: (sundy) we use reinterpret_cast here, it gains much performance
        for value in array.values().iter() {
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
        for (value, vec) in self.into_no_null_iter().zip(vec.iter_mut()) {
            BinaryWrite::write_scalar(vec, value)?;
        }
        Ok(())
    }
}

impl GroupHash for DFBooleanArray {
    fn fixed_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.inner();
        let mut ptr = ptr;

        for value in array.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(&(value as u8) as *const u8, ptr, 1);
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

impl GroupHash for DFStringArray {
    fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        assert_eq!(vec.len(), self.len());
        for (value, vec) in self.into_no_null_iter().zip(vec.iter_mut()) {
            BinaryWrite::write_binary(vec, &value)?;
        }
        Ok(())
    }
}

impl GroupHash for DFListArray {}
impl GroupHash for DFNullArray {}
impl GroupHash for DFStructArray {}
