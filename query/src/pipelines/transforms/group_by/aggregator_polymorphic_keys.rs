use common_datablocks::{HashMethod, HashMethodFixedKeys, HashMethodSerializer};
use common_datavalues::arrays::{DataArray, PrimitiveArrayBuilder, ArrayBuilder};
use common_datavalues::series::Series;
use common_datavalues::{DFNumericType, DFPrimitiveType};
use std::hash::Hash;
use common_datavalues::prelude::IntoSeries;
use std::fmt::Debug;
use std::marker::PhantomData;
use crate::common::HashMap;

pub trait PolymorphicKeysHelper<Method: HashMethod> {
    type ArrayBuilder: BinaryKeysArrayBuilder<Method>;

    // fn aggregator_container(&self) -> HashMap<Method::HashKey, usize>;

    fn binary_keys_array_builder(&self, capacity: usize) -> Self::ArrayBuilder;
}

impl<T> PolymorphicKeysHelper<Self> for HashMethodFixedKeys<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Hash + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
{
    type ArrayBuilder = NativeBinaryKeysArrayBuilder<T>;

    // fn aggregator_container(&self) -> HashMap<common_datablocks::kernels::data_block_group_by_hash::HashKey, usize> {
    //     todo!()
    // }

    fn binary_keys_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
        NativeBinaryKeysArrayBuilder::<T> {
            inner_builder: PrimitiveArrayBuilder::<T>::with_capacity(capacity)
        }
    }
}

// impl PolymorphicKeysHelper<HashMethodSerializer> for HashMethodSerializer {
//     type Builder = ();
//
//     fn binary_keys_array_builder(&self, capacity: usize) -> Self::Builder {
//         unimplemented!()
//     }
// }

pub trait BinaryKeysArrayBuilder<Method: HashMethod> {
    fn finish(self) -> Series;
    fn append_value(&mut self, v: Method::HashKey);
}

pub struct NativeBinaryKeysArrayBuilder<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Hash + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
{
    inner_builder: PrimitiveArrayBuilder<T>,
}

impl<T> BinaryKeysArrayBuilder<HashMethodFixedKeys<T>> for NativeBinaryKeysArrayBuilder<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Hash + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
{
    #[inline]
    fn finish(mut self) -> Series {
        self.inner_builder.finish().array.into_series()
    }

    #[inline]
    fn append_value(&mut self, v: <HashMethodFixedKeys<T> as HashMethod>::HashKey) {
        self.inner_builder.append_value(v)
    }
}

