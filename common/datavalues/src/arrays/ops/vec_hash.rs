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
use std::hash::Hash;
use std::hash::Hasher;

use common_exception::ErrorCode;
use common_exception::Result;

use super::ArrayApply;
use crate::prelude::*;

pub trait VecHash: Debug {
    /// Compute the hash for all values in the array.
    fn vec_hash(&self, _hasher: DFHasher) -> Result<DFUInt64Array> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply vec_hash operation for {:?}",
            self,
        )))
    }
}

impl<T> VecHash for DFPrimitiveArray<T>
where
    T: DFIntegerType,
    T: Hash,
{
    fn vec_hash(&self, hasher: DFHasher) -> Result<DFUInt64Array> {
        Ok(self.apply_cast_numeric(|v| {
            let mut h = hasher.clone_initial();

            v.hash(&mut h);
            h.finish()
        }))
    }
}

impl VecHash for DFUtf8Array {
    fn vec_hash(&self, hasher: DFHasher) -> Result<DFUInt64Array> {
        Ok(self.apply_cast_numeric(|v| {
            let mut h = hasher.clone_initial();
            v.hash(&mut h);
            h.finish()
        }))
    }
}

impl VecHash for DFBooleanArray {
    fn vec_hash(&self, hasher: DFHasher) -> Result<DFUInt64Array> {
        Ok(self.apply_cast_numeric(|v| {
            let mut h = hasher.clone_initial();
            v.hash(&mut h);
            h.finish()
        }))
    }
}

impl VecHash for DFFloat32Array {
    fn vec_hash(&self, hasher: DFHasher) -> Result<DFUInt64Array> {
        Ok(self.apply_cast_numeric(|v| {
            let v = v.to_bits();
            let mut h = hasher.clone_initial();
            v.hash(&mut h);
            h.finish()
        }))
    }
}
impl VecHash for DFFloat64Array {
    fn vec_hash(&self, hasher: DFHasher) -> Result<DFUInt64Array> {
        Ok(self.apply_cast_numeric(|v| {
            let v = v.to_bits();
            let mut h = hasher.clone_initial();
            v.hash(&mut h);
            h.finish()
        }))
    }
}

impl VecHash for DFBinaryArray {
    fn vec_hash(&self, hasher: DFHasher) -> Result<DFUInt64Array> {
        let binary_data = self.get_inner();
        let mut builder = PrimitiveArrayBuilder::<u64>::with_capacity(self.len());

        (0..self.len()).for_each(|index| {
            if self.is_null(index) {
                builder.append_null();
            } else {
                let mut h = hasher.clone_initial();
                h.write(binary_data.value(index));
                builder.append_value(h.finish());
            }
        });

        Ok(builder.finish())
    }
}

impl VecHash for DFListArray {
    fn vec_hash(&self, _hasher: DFHasher) -> Result<DFUInt64Array> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply vec_hash operation for {:?}",
            self,
        )))
    }
}

impl VecHash for DFStructArray {}

impl VecHash for DFNullArray {}
