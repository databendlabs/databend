use std::hash::Hash;
use std::hash::Hasher;

use super::ArrayApply;
use crate::arrays::DataArray;
use crate::DFBinaryArray;
use crate::DFBooleanArray;
use crate::DFFloat32Array;
use crate::DFFloat64Array;
use crate::DFHasher;
use crate::DFIntegerType;
use crate::DFListArray;
use crate::DFNullArray;
use crate::DFStructArray;
use crate::DFUInt64Array;
use crate::DFUtf8Array;

// Read more:
//  https://www.cockroachlabs.com/blog/vectorized-hash-joiner/
//  http://myeyesareblind.com/2017/02/06/Combine-hash-values/

pub trait VecHash {
    /// Compute the hash for all values in the array.
    fn vec_hash(&self, _hasher: DFHasher) -> DFUInt64Array {
        unimplemented!()
    }
}

impl<T> VecHash for DataArray<T>
where
    T: DFIntegerType,
    T::Native: Hash,
{
    fn vec_hash(&self, hasher: DFHasher) -> DFUInt64Array {
        self.apply_cast_numeric(|v| {
            let mut h = hasher.clone_initial();

            v.hash(&mut h);
            h.finish()
        })
    }
}

impl VecHash for DFUtf8Array {
    fn vec_hash(&self, hasher: DFHasher) -> DFUInt64Array {
        self.apply_cast_numeric(|v| {
            let mut h = hasher.clone_initial();
            v.hash(&mut h);
            h.finish()
        })
    }
}

impl VecHash for DFBooleanArray {
    fn vec_hash(&self, hasher: DFHasher) -> DFUInt64Array {
        self.apply_cast_numeric(|v| {
            let mut h = hasher.clone_initial();
            v.hash(&mut h);
            h.finish()
        })
    }
}

impl VecHash for DFFloat32Array {
    fn vec_hash(&self, hasher: DFHasher) -> DFUInt64Array {
        self.apply_cast_numeric(|v| {
            let v = v.to_bits();
            let mut h = hasher.clone_initial();
            v.hash(&mut h);
            h.finish()
        })
    }
}
impl VecHash for DFFloat64Array {
    fn vec_hash(&self, hasher: DFHasher) -> DFUInt64Array {
        self.apply_cast_numeric(|v| {
            let v = v.to_bits();
            let mut h = hasher.clone_initial();
            v.hash(&mut h);
            h.finish()
        })
    }
}

impl VecHash for DFListArray {}
impl VecHash for DFBinaryArray {}
impl VecHash for DFNullArray {}
impl VecHash for DFStructArray {}
