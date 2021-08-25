use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use common_datablocks::{HashMethod, HashMethodFixedKeys, HashMethodSerializer};
use common_datavalues::{DFNumericType, DFPrimitiveType};
use common_datavalues::arrays::{ArrayBuilder, DataArray, PrimitiveArrayBuilder};
use common_datavalues::prelude::IntoSeries;
use common_datavalues::series::Series;

use crate::common::{HashMap, HashTable, HashTableKeyable};
use crate::pipelines::transforms::group_by::AggregatorDataState;
use crate::pipelines::transforms::group_by::aggregator_container::NativeAggregatorDataContainer;

pub trait PolymorphicKeysHelper<Method: HashMethod> where Method::HashKey: HashTableKeyable {
    type DataContainer: AggregatorDataState<Method>;
    fn aggregate_state(&self) -> Self::DataContainer;

    type ArrayBuilder: BinaryKeysArrayBuilder<Method>;
    fn binary_keys_array_builder(&self, capacity: usize) -> Self::ArrayBuilder;
}

impl<T> PolymorphicKeysHelper<Self> for HashMethodFixedKeys<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
    NativeAggregatorDataContainer<T>: AggregatorDataState<HashMethodFixedKeys<T>>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable
{
    type DataContainer = NativeAggregatorDataContainer<T>;
    fn aggregate_state(&self) -> Self::DataContainer {
        NativeAggregatorDataContainer::<T> {
            data: HashTable::create(),
        }
    }

    type ArrayBuilder = NativeBinaryKeysArrayBuilder<T>;
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
    T::Native: HashTableKeyable,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
{
    inner_builder: PrimitiveArrayBuilder<T>,
}

impl<T> BinaryKeysArrayBuilder<HashMethodFixedKeys<T>> for NativeBinaryKeysArrayBuilder<T> where
    T: DFNumericType,
    T::Native: HashTableKeyable,
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
