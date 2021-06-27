use std::convert::TryInto;
use std::hash::BuildHasher;
use std::hash::BuildHasherDefault;
use std::hash::Hash;
use std::hash::Hasher;

use ahash::RandomState;
use common_arrow::arrow::array::ArrayRef;

use super::ArrayApply;
use crate::arrays::DataArray;
use crate::DFBinaryArray;
use crate::DFBooleanArray;
use crate::DFFloat32Array;
use crate::DFFloat64Array;
use crate::DFIntegerType;
use crate::DFListArray;
use crate::DFNullArray;
use crate::DFStringArray;
use crate::DFStructArray;
use crate::DFUInt64Array;

// Read more:
//  https://www.cockroachlabs.com/blog/vectorized-hash-joiner/
//  http://myeyesareblind.com/2017/02/06/Combine-hash-values/

pub trait VecHash {
    /// Compute the hash for all values in the array.
    ///
    /// This currently only works with the AHash RandomState hasher builder.
    fn vec_hash(&self, _random_state: RandomState) -> DFUInt64Array {
        unimplemented!()
    }
}

impl<T> VecHash for DataArray<T>
where
    T: DFIntegerType,
    T::Native: Hash,
{
    //TODO: maybe better just to use the native value as u64
    fn vec_hash(&self, random_state: RandomState) -> DFUInt64Array {
        // Note that we don't use the no null branch! This can break in unexpected ways.
        // for instance with threading we split an array in n_threads, this may lead to
        // splits that have no nulls and splits that have nulls. Then one array is hashed with
        // Option<T> and the other array with T.
        // Meaning that they cannot be compared. By always hashing on Option<T> the random_state is
        // the only deterministic seed.
        self.branch_apply_cast_numeric_no_null(|opt_v| {
            let mut hasher = random_state.build_hasher();
            opt_v.hash(&mut hasher);
            hasher.finish()
        })
    }
}

impl VecHash for DFStringArray {
    fn vec_hash(&self, random_state: RandomState) -> DFUInt64Array {
        self.branch_apply_cast_numeric_no_null(|opt_v| {
            let mut hasher = random_state.build_hasher();
            opt_v.hash(&mut hasher);
            hasher.finish()
        })
    }
}

impl VecHash for DFBooleanArray {
    fn vec_hash(&self, random_state: RandomState) -> DFUInt64Array {
        self.branch_apply_cast_numeric_no_null(|opt_v| {
            let mut hasher = random_state.build_hasher();
            opt_v.hash(&mut hasher);
            hasher.finish()
        })
    }
}

impl VecHash for DFFloat32Array {
    fn vec_hash(&self, random_state: RandomState) -> DFUInt64Array {
        self.branch_apply_cast_numeric_no_null(|opt_v| {
            let opt_v = opt_v.map(|v| v.to_bits());
            let mut hasher = random_state.build_hasher();
            opt_v.hash(&mut hasher);
            hasher.finish()
        })
    }
}
impl VecHash for DFFloat64Array {
    fn vec_hash(&self, random_state: RandomState) -> DFUInt64Array {
        self.branch_apply_cast_numeric_no_null(|opt_v| {
            let opt_v = opt_v.map(|v| v.to_bits());
            let mut hasher = random_state.build_hasher();
            opt_v.hash(&mut hasher);
            hasher.finish()
        })
    }
}

impl VecHash for DFListArray {}
impl VecHash for DFBinaryArray {}
impl VecHash for DFNullArray {}
impl VecHash for DFStructArray {}
