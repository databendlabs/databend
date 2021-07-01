use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use common_exception::Result;
use num::NumCast;

use super::ArrayCast;
use crate::arrays::DataArray;
use crate::prelude::*;
use crate::DFBinaryArray;
use crate::DFBooleanArray;
use crate::DFFloat32Array;
use crate::DFFloat64Array;
use crate::DFIntegerType;
use crate::DFListArray;
use crate::DFNullArray;
use crate::DFStructArray;
use crate::DFUInt64Array;
use crate::DFUtf8Array;

// Read more:
//  https://www.cockroachlabs.com/blog/vectorized-hash-joiner/
//  http://myeyesareblind.com/2017/02/06/Combine-hash-values/

pub trait GroupHash {
    /// Compute the hash for all values in the array.
    fn group_hash(&self) -> Result<DFUInt64Array> {
        unimplemented!()
    }
}

impl<T> GroupHash for DataArray<T>
where
    T: DFIntegerType,
    T::Native: Hash,
    T::Native: NumCast,
{
    fn group_hash(&self) -> Result<DFUInt64Array> {
        self.cast()
    }
}

impl GroupHash for DFUtf8Array {
    fn group_hash(&self) -> Result<DFUInt64Array> {
        Ok(self.apply_cast_numeric(|v| {
            let mut hasher = DefaultHasher::new();
            hasher.write(v.as_bytes());

            let c = hasher.finish();
            c
        }))
    }
}

impl GroupHash for DFBooleanArray {
    fn group_hash(&self) -> Result<DFUInt64Array> {
        self.cast()
    }
}

impl GroupHash for DFFloat32Array {
    fn group_hash(&self) -> Result<DFUInt64Array> {
        Ok(self.apply_cast_numeric(|v| {
            let mut hasher = DefaultHasher::new();
            hasher.write_u32(v.to_bits());
            hasher.finish()
        }))
    }
}
impl GroupHash for DFFloat64Array {
    fn group_hash(&self) -> Result<DFUInt64Array> {
        Ok(self.apply_cast_numeric(|v| {
            let mut hasher = DefaultHasher::new();
            hasher.write_u64(v.to_bits());
            hasher.finish()
        }))
    }
}

impl GroupHash for DFListArray {}
impl GroupHash for DFBinaryArray {}
impl GroupHash for DFNullArray {}
impl GroupHash for DFStructArray {}
