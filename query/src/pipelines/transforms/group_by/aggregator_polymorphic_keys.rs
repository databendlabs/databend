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
use crate::pipelines::transforms::group_by::aggregator_state::{FixedKeysAggregatorState, SerializedKeysAggregatorState};
use crate::pipelines::transforms::group_by::aggregator_keys_builder::{KeysArrayBuilder, FixedKeysArrayBuilder, SerializedKeysArrayBuilder};
use crate::pipelines::transforms::group_by::AggregatorState;

pub trait PolymorphicKeysHelper<Method: HashMethod> {
    type State: AggregatorState<Method>;
    fn aggregate_state(&self) -> Self::State;

    type ArrayBuilder: KeysArrayBuilder<<Self::State as AggregatorState<Method>>::HashKeyState>;
    fn state_array_builder(&self, capacity: usize, state: &Self::State) -> Self::ArrayBuilder;
}

impl<T> PolymorphicKeysHelper<Self> for HashMethodFixedKeys<T> where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey=T::Native>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable,
    FixedKeysAggregatorState<T>: AggregatorState<HashMethodFixedKeys<T>, HashKeyState=T::Native>,
{
    type State = FixedKeysAggregatorState<T>;
    fn aggregate_state(&self) -> Self::State {
        FixedKeysAggregatorState::<T> {
            area: Bump::new(),
            data: HashTable::create(),
        }
    }

    type ArrayBuilder = FixedKeysArrayBuilder<T>;
    fn state_array_builder(&self, capacity: usize, _: &Self::State) -> Self::ArrayBuilder {
        FixedKeysArrayBuilder::<T> {
            inner_builder: PrimitiveArrayBuilder::<T>::with_capacity(capacity)
        }
    }
}

impl PolymorphicKeysHelper<HashMethodSerializer> for HashMethodSerializer {
    type State = SerializedKeysAggregatorState;
    fn aggregate_state(&self) -> Self::State {
        unimplemented!()
    }

    type ArrayBuilder = SerializedKeysArrayBuilder;
    fn state_array_builder(&self, capacity: usize, state: &Self::State) -> Self::ArrayBuilder {
        unimplemented!()
    }
}
