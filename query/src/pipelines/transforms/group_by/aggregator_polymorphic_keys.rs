use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use bumpalo::Bump;

use common_datablocks::{HashMethod, HashMethodFixedKeys, HashMethodSerializer};
use common_datavalues::{DFNumericType, DFPrimitiveType};
use common_datavalues::arrays::{ArrayBuilder, DataArray, PrimitiveArrayBuilder};
use common_datavalues::prelude::IntoSeries;
use common_datavalues::series::Series;

use crate::common::{HashMap, HashTable, HashTableKeyable};
use crate::pipelines::transforms::group_by::aggregator_container::{NativeAggregatorDataContainer, SerializedAggregatorDataContainer};
use crate::pipelines::transforms::group_by::aggregator_keys::{BinaryKeysArrayBuilder, NativeBinaryKeysArrayBuilder, SerializedBinaryKeysArrayBuilder};
use crate::pipelines::transforms::group_by::AggregatorDataState;

pub trait PolymorphicKeysHelper<Method: HashMethod> where Method::HashKey: HashTableKeyable {
    type State: AggregatorDataState<Method>;
    fn aggregate_state(&self) -> Self::State;

    // TODO: need aggregate state
    type ArrayBuilder: BinaryKeysArrayBuilder<<Self::State as AggregatorDataState<Method>>::HashKeyState>;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder;
}

impl<T> PolymorphicKeysHelper<Self> for HashMethodFixedKeys<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable,
    NativeAggregatorDataContainer<T>: AggregatorDataState<HashMethodFixedKeys<T>, HashKeyState=T::Native>,
{
    type State = NativeAggregatorDataContainer<T>;
    fn aggregate_state(&self) -> Self::State {
        NativeAggregatorDataContainer::<T> {
            area: Bump::new(),
            data: HashTable::create(),
        }
    }

    type ArrayBuilder = NativeBinaryKeysArrayBuilder<T>;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
        NativeBinaryKeysArrayBuilder::<T> {
            inner_builder: PrimitiveArrayBuilder::<T>::with_capacity(capacity)
        }
    }
}

impl PolymorphicKeysHelper<HashMethodSerializer> for HashMethodSerializer {
    type State = SerializedAggregatorDataContainer;
    fn aggregate_state(&self) -> Self::State {
        unimplemented!()
    }

    type ArrayBuilder = SerializedBinaryKeysArrayBuilder;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
        unimplemented!()
    }
}
