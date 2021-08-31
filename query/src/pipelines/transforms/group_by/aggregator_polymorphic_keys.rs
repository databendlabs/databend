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
use common_datablocks::{HashMethod, HashMethodKeysU8, HashMethodKeysU16, HashMethodKeysU32, HashMethodKeysU64};
use common_datablocks::HashMethodFixedKeys;
use common_datablocks::HashMethodSerializer;
use common_datavalues::arrays::BinaryArrayBuilder;
use common_datavalues::arrays::PrimitiveArrayBuilder;
use common_datavalues::prelude::DFPrimitiveArray;
use common_datavalues::series::IntoSeries;
use common_datavalues::DFPrimitiveType;

use crate::common::{HashTable, HashTableEntity, KeyValueEntity};
use crate::common::HashTableKeyable;
use crate::pipelines::transforms::group_by::aggregator_keys_builder::FixedKeysArrayBuilder;
use crate::pipelines::transforms::group_by::aggregator_keys_builder::KeysArrayBuilder;
use crate::pipelines::transforms::group_by::aggregator_keys_builder::SerializedKeysArrayBuilder;
use crate::pipelines::transforms::group_by::aggregator_state::{LongerFixedKeysAggregatorState, ShortFixedKeysAggregatorState};
use crate::pipelines::transforms::group_by::aggregator_state::SerializedKeysAggregatorState;
use crate::pipelines::transforms::group_by::AggregatorState;
use std::marker::PhantomData;

// Provide functions for all HashMethod to help implement polymorphic group by key
//
// When we want to add new HashMethod, we need to add the following components
//     - HashMethod, more information in [HashMethod] trait
//     - AggregatorState, more information in [AggregatorState] trait
//     - KeysArrayBuilder, more information in [KeysArrayBuilder] trait
//     - PolymorphicKeysHelper, more information in following comments
//
// For example:
//
// use bumpalo::Bump;
// use datafuse_query::common::HashTable;
// use common_datablocks::HashMethodSerializer;
// use common_datavalues::arrays::BinaryArrayBuilder;
// use datafuse_query::pipelines::transforms::group_by::PolymorphicKeysHelper;
// use datafuse_query::pipelines::transforms::group_by::aggregator_state::SerializedKeysAggregatorState;
// use datafuse_query::pipelines::transforms::group_by::aggregator_keys_builder::SerializedKeysArrayBuilder;
//
// impl PolymorphicKeysHelper<HashMethodSerializer> for HashMethodSerializer {
//     type State = SerializedKeysAggregatorState;
//     fn aggregate_state(&self) -> Self::State {
//         SerializedKeysAggregatorState {
//             keys_area: Bump::new(),
//             state_area: Bump::new(),
//             data_state_map: HashTable::create(),
//         }
//     }
//
//     type ArrayBuilder = SerializedKeysArrayBuilder;
//     fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
//         SerializedKeysArrayBuilder {
//             inner_builder: BinaryArrayBuilder::with_capacity(capacity),
//         }
//     }
// }
//
pub trait PolymorphicKeysHelper<Method: HashMethod> {
    type State: AggregatorState<Method>;
    fn aggregate_state(&self) -> Self::State;

    type ArrayBuilder: KeysArrayBuilder<<Self::State as AggregatorState<Method>>::Key>;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder;
}

impl PolymorphicKeysHelper<HashMethodKeysU8> for HashMethodKeysU8 {
    type State = ShortFixedKeysAggregatorState<u8>;
    fn aggregate_state(&self) -> Self::State {
        Self::State::create(u8::MAX as usize)
    }

    type ArrayBuilder = FixedKeysArrayBuilder<u8>;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
        FixedKeysArrayBuilder::<u8> {
            inner_builder: PrimitiveArrayBuilder::<u8>::with_capacity(capacity),
        }
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU16> for HashMethodKeysU16 {
    type State = ShortFixedKeysAggregatorState<u16>;
    fn aggregate_state(&self) -> Self::State {
        Self::State::create(u16::MAX as usize)
    }

    type ArrayBuilder = FixedKeysArrayBuilder<u16>;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
        FixedKeysArrayBuilder::<u16> {
            inner_builder: PrimitiveArrayBuilder::<u16>::with_capacity(capacity),
        }
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU32> for HashMethodKeysU32 {
    type State = LongerFixedKeysAggregatorState<u32>;
    fn aggregate_state(&self) -> Self::State {
        LongerFixedKeysAggregatorState::<u32> {
            area: Bump::new(),
            data: HashTable::create(),
        }
    }

    type ArrayBuilder = FixedKeysArrayBuilder<u32>;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
        FixedKeysArrayBuilder::<u32> {
            inner_builder: PrimitiveArrayBuilder::<u32>::with_capacity(capacity),
        }
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU64> for HashMethodKeysU64 {
    type State = LongerFixedKeysAggregatorState<u64>;
    fn aggregate_state(&self) -> Self::State {
        LongerFixedKeysAggregatorState::<u64> {
            area: Bump::new(),
            data: HashTable::create(),
        }
    }

    type ArrayBuilder = FixedKeysArrayBuilder<u64>;
    fn state_array_builder(&self, capacity: usize) -> Self::ArrayBuilder {
        FixedKeysArrayBuilder::<u64> {
            inner_builder: PrimitiveArrayBuilder::<u64>::with_capacity(capacity),
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
