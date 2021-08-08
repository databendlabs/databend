// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use std::fmt::Debug;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::*;

// Read more:
//  https://www.cockroachlabs.com/blog/vectorized-hash-joiner/
//  http://myeyesareblind.com/2017/02/06/Combine-hash-values/

pub trait GroupHash: Debug {
    /// Compute the hash for all values in the array.
    fn group_hash(&self, _ptr: *mut u8, _step: usize) -> Result<()> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply  fn group_hash operation for {:?}",
            self,
        )))
    }
}

impl GroupHash for DFUInt8Array {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;
        for value in array.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(value as *const u8, ptr, 1);
                ptr = ptr.add(step);
            }
        }
        Ok(())
    }
}

impl GroupHash for DFInt8Array {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        for value in array.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(value as *const i8 as *const u8, ptr, 1);
                ptr = ptr.add(step);
            }
        }
        Ok(())
    }
}

impl GroupHash for DFUInt16Array {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        for value in array.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(value as *const u16 as *const u8, ptr, 2);
                ptr = ptr.add(step);
            }
        }

        Ok(())
    }
}

impl GroupHash for DFInt16Array {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        for value in array.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(value as *const i16 as *const u8, ptr, 2);
                ptr = ptr.add(step);
            }
        }
        Ok(())
    }
}

impl GroupHash for DFInt32Array {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        for value in array.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(value as *const i32 as *const u8, ptr, 4);
                ptr = ptr.add(step);
            }
        }

        Ok(())
    }
}

impl GroupHash for DFUInt32Array {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        for value in array.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(value as *const u32 as *const u8, ptr, 4);
                ptr = ptr.add(step);
            }
        }

        Ok(())
    }
}
impl GroupHash for DFBooleanArray {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        let rows = self.len();
        unsafe {
            for i in 0..rows {
                let value = array.value_unchecked(i) as u8;
                std::ptr::copy_nonoverlapping(&value as *const u8, ptr, 1);
                ptr = ptr.add(step);
            }
        }

        Ok(())
    }
}

impl GroupHash for DFFloat32Array {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        for value in array.values().iter() {
            unsafe {
                let bits = value.to_bits();
                std::ptr::copy_nonoverlapping(&bits as *const u32 as *const u8, ptr, 4);
                ptr = ptr.add(step);
            }
        }
        Ok(())
    }
}
impl GroupHash for DFFloat64Array {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        for value in array.values().iter() {
            unsafe {
                let bits = value.to_bits();
                std::ptr::copy_nonoverlapping(&bits as *const u64 as *const u8, ptr, 8);
                ptr = ptr.add(step);
            }
        }

        Ok(())
    }
}

impl GroupHash for DFUInt64Array {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        for value in array.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(value as *const u64 as *const u8, ptr, 8);
                ptr = ptr.add(step);
            }
        }

        Ok(())
    }
}
impl GroupHash for DFInt64Array {
    fn group_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
        let array = self.downcast_ref();
        let mut ptr = ptr;

        for value in array.values().iter() {
            unsafe {
                std::ptr::copy_nonoverlapping(value as *const i64 as *const u8, ptr, 8);
                ptr = ptr.add(step);
            }
        }

        Ok(())
    }
}

impl GroupHash for DFListArray {}
impl GroupHash for DFUtf8Array {}
impl GroupHash for DFBinaryArray {}
impl GroupHash for DFNullArray {}
impl GroupHash for DFStructArray {}
