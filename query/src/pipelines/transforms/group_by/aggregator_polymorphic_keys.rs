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

use bumpalo::Bump;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datablocks::HashMethodSerializer;
use common_datavalues::arrays::BinaryArrayBuilder;
use common_datavalues::arrays::PrimitiveArrayBuilder;
use common_datavalues::DFNumericType;

use crate::common::HashTable;
use crate::common::HashTableKeyable;
use crate::pipelines::transforms::group_by::aggregator_keys_builder::FixedKeysArrayBuilder;
use crate::pipelines::transforms::group_by::aggregator_keys_builder::KeysArrayBuilder;
use crate::pipelines::transforms::group_by::aggregator_keys_builder::SerializedKeysArrayBuilder;
use crate::pipelines::transforms::group_by::aggregator_state::FixedKeysAggregatorState;
use crate::pipelines::transforms::group_by::aggregator_state::SerializedKeysAggregatorState;
use crate::pipelines::transforms::group_by::AggregatorState;

/// Provide functions for all HashMethod to help implement polymorphic group by key
///
/// When we want to add new HashMethod, we need to add the following components
///     - HashMethod, more information in [HashMethod] trait
///     - AggregatorState, more information in [AggregatorState] trait
///     - KeysArrayBuilder, more information in [KeysArrayBuilder] trait
///     - PolymorphicKeysHelper, more information in following comments
///
/// For example:
///
/// impl PolymorphicKeysHelper<NewHashMethod> for NewHashMethod {
///     type State: NewHashMethodDataState;
///     fn aggregate_state(&self) -> Self::State {
///         NewHashMethodDataState::create()
///     }
///
///     type ArrayBuilder: NewHashMethodArrayBuilder;
///     fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
///         NewHashMethodArrayBuilder::create()
///     }
/// }
///
pub trait PolymorphicKeysHelper<Method: HashMethod> {
    type State: AggregatorState<Method>;
    fn aggregate_state(&self) -> Self::State;

    type ArrayBuilder: KeysArrayBuilder<<Self::State as AggregatorState<Method>>::HashKeyState>;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder;
}

impl<T> PolymorphicKeysHelper<Self> for HashMethodFixedKeys<T>
where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Clone + Debug,
    HashMethodFixedKeys<T>: HashMethod<HashKey = T::Native>,
    <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashTableKeyable,
    FixedKeysAggregatorState<T>: AggregatorState<HashMethodFixedKeys<T>, HashKeyState = T::Native>,
{
    type State = FixedKeysAggregatorState<T>;
    fn aggregate_state(&self) -> Self::State {
        FixedKeysAggregatorState::<T> {
            area: Bump::new(),
            data: HashTable::create(),
        }
    }

    type ArrayBuilder = FixedKeysArrayBuilder<T>;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
        FixedKeysArrayBuilder::<T> {
            inner_builder: PrimitiveArrayBuilder::<T>::with_capacity(capacity),
        }
    }
}

impl PolymorphicKeysHelper<HashMethodSerializer> for HashMethodSerializer {
    type State = SerializedKeysAggregatorState;
    fn aggregate_state(&self) -> Self::State {
        SerializedKeysAggregatorState {
            keys_area: Bump::new(),
            state_area: Bump::new(),
            data_state_map: HashTable::create(),
        }
    }

    type ArrayBuilder = SerializedKeysArrayBuilder;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
        SerializedKeysArrayBuilder {
            inner_builder: BinaryArrayBuilder::with_capacity(capacity),
        }
    }
}
