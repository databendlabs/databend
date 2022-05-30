// Copyright 2021 Datafuse Labs.
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

use std::marker::PhantomData;

use bumpalo::Bump;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datablocks::HashMethodKeysU128;
use common_datablocks::HashMethodSerializer;
use common_datablocks::HashMethodSingleString;
use common_datavalues::prelude::*;
use common_exception::Result;

use super::aggregator_groups_builder::SingleStringGroupColumnsBuilder;
use super::aggregator_keys_builder::LargeFixedKeysColumnBuilder;
use super::aggregator_keys_iter::LargeFixedKeysColumnIter;
use crate::common::HashMapKind;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::transforms::group_by::aggregator_groups_builder::FixedKeysGroupColumnsBuilder;
use crate::pipelines::transforms::group_by::aggregator_groups_builder::GroupColumnsBuilder;
use crate::pipelines::transforms::group_by::aggregator_groups_builder::SerializedKeysGroupColumnsBuilder;
use crate::pipelines::transforms::group_by::aggregator_keys_builder::FixedKeysColumnBuilder;
use crate::pipelines::transforms::group_by::aggregator_keys_builder::KeysColumnBuilder;
use crate::pipelines::transforms::group_by::aggregator_keys_builder::SerializedKeysColumnBuilder;
use crate::pipelines::transforms::group_by::aggregator_keys_iter::FixedKeysColumnIter;
use crate::pipelines::transforms::group_by::aggregator_keys_iter::KeysColumnIter;
use crate::pipelines::transforms::group_by::aggregator_keys_iter::SerializedKeysColumnIter;
use crate::pipelines::transforms::group_by::aggregator_state::LongerFixedKeysAggregatorState;
use crate::pipelines::transforms::group_by::aggregator_state::SerializedKeysAggregatorState;
use crate::pipelines::transforms::group_by::aggregator_state::ShortFixedKeysAggregatorState;
use crate::pipelines::transforms::group_by::AggregatorState;

// Provide functions for all HashMethod to help implement polymorphic group by key
//
// When we want to add new HashMethod, we need to add the following components
//     - HashMethod, more information in [HashMethod] trait
//     - AggregatorState, more information in [AggregatorState] trait
//     - KeysColumnBuilder, more information in [KeysColumnBuilder] trait
//     - PolymorphicKeysHelper, more information in following comments
//
// For example:
//
// use bumpalo::Bump;
// use databend_query::common::HashTable;
// use common_datablocks::HashMethodSerializer;
// use common_datavalues::prelude::*;
// use databend_query::pipelines::transforms::group_by::PolymorphicKeysHelper;
// use databend_query::pipelines::transforms::group_by::aggregator_state::SerializedKeysAggregatorState;
// use databend_query::pipelines::transforms::group_by::aggregator_keys_builder::SerializedKeysColumnBuilder;
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
//     type ColumnBuilder = SerializedKeysColumnBuilder;
//     fn state_array_builder(&self, capacity: usize) -> Self::ColumnBuilder {
//         SerializedKeysColumnBuilder {
//             inner_builder: MutableStringColumn::with_capacity(capacity),
//         }
//     }
// }
//
pub trait PolymorphicKeysHelper<Method: HashMethod> {
    type State: AggregatorState<Method>;
    fn aggregate_state(&self) -> Self::State;

    type ColumnBuilder: KeysColumnBuilder<<Self::State as AggregatorState<Method>>::Key>;
    fn keys_column_builder(&self, capacity: usize) -> Self::ColumnBuilder;

    type KeysColumnIter: KeysColumnIter<<Self::State as AggregatorState<Method>>::Key>;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter>;

    type GroupColumnsBuilder: GroupColumnsBuilder<<Self::State as AggregatorState<Method>>::Key>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> Self::GroupColumnsBuilder;
}

macro_rules! dispatch_unsigned_short_types{
    ($t: ty, $state: ty) => {
        impl PolymorphicKeysHelper<HashMethodFixedKeys<$t>> for HashMethodFixedKeys<$t> {
           
            type State = $state;
            fn aggregate_state(&self) -> Self::State {
                Self::State::create((u8::MAX as usize) + 1) 
            }

            type ColumnBuilder = FixedKeysColumnBuilder<$t>;
            fn keys_column_builder(&self, capacity: usize) -> Self::ColumnBuilder {
                FixedKeysColumnBuilder::<$t> {
                    inner_builder: MutablePrimitiveColumn::<$t>::with_capacity(capacity),
                }
            }

            type KeysColumnIter = FixedKeysColumnIter<$t>;
            fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
                FixedKeysColumnIter::create(Series::check_get::<PrimitiveColumn<$t>>(column)?)
            }

            type GroupColumnsBuilder = FixedKeysGroupColumnsBuilder<$t>;
            fn group_columns_builder(
                &self,
                capacity: usize,
                params: &AggregatorParams,
            ) -> Self::GroupColumnsBuilder {
                FixedKeysGroupColumnsBuilder::<$t>::create(capacity, params)
            }
        }
    };
}

type StateU8 = ShortFixedKeysAggregatorState<u8>;
dispatch_unsigned_short_types! {u8, StateU8 }

type StateU16 = ShortFixedKeysAggregatorState<u16>;
dispatch_unsigned_short_types! {u16, StateU16 }


macro_rules! dispatch_unsigned_long_types{
    ($t: ty, $state: ty) => {
        impl PolymorphicKeysHelper<HashMethodFixedKeys<$t>> for HashMethodFixedKeys<$t> {
           
            type State = $state;
            fn aggregate_state(&self) -> Self::State {
                 Self::State::default()
            }

            type ColumnBuilder = FixedKeysColumnBuilder<$t>;
            fn keys_column_builder(&self, capacity: usize) -> Self::ColumnBuilder {
                FixedKeysColumnBuilder::<$t> {
                    inner_builder: MutablePrimitiveColumn::<$t>::with_capacity(capacity),
                }
            }

            type KeysColumnIter = FixedKeysColumnIter<$t>;
            fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
                FixedKeysColumnIter::create(Series::check_get::<PrimitiveColumn<$t>>(column)?)
            }

            type GroupColumnsBuilder = FixedKeysGroupColumnsBuilder<$t>;
            fn group_columns_builder(
                &self,
                capacity: usize,
                params: &AggregatorParams,
            ) -> Self::GroupColumnsBuilder {
                FixedKeysGroupColumnsBuilder::<$t>::create(capacity, params)
            }
        }
    };
}

type StateU32 = LongerFixedKeysAggregatorState<u32>;
dispatch_unsigned_long_types! {u32, StateU32 }
type StateU64 = LongerFixedKeysAggregatorState<u64>;
dispatch_unsigned_long_types! {u64, StateU64}
 
 
impl PolymorphicKeysHelper<HashMethodKeysU128> for HashMethodKeysU128 {
    type State = LongerFixedKeysAggregatorState<u128>;
    fn aggregate_state(&self) -> Self::State {
        Self::State::default()
    }

    type ColumnBuilder = LargeFixedKeysColumnBuilder<u128>;
    fn keys_column_builder(&self, capacity: usize) -> Self::ColumnBuilder {
        LargeFixedKeysColumnBuilder {
            inner_builder: MutableStringColumn::with_capacity(capacity),
            _t: PhantomData,
        }
    }

    type KeysColumnIter = LargeFixedKeysColumnIter<u128>;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        LargeFixedKeysColumnIter::create(Series::check_get::<StringColumn>(column)?)
    }

    type GroupColumnsBuilder = FixedKeysGroupColumnsBuilder<u128>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> Self::GroupColumnsBuilder {
        FixedKeysGroupColumnsBuilder::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodSingleString> for HashMethodSingleString {
    type State = SerializedKeysAggregatorState;
    fn aggregate_state(&self) -> Self::State {
        SerializedKeysAggregatorState {
            keys_area: Bump::new(),
            state_area: Bump::new(),
            data_state_map: HashMapKind::create_hash_table(),
            two_level_flag: false,
        }
    }

    type ColumnBuilder = SerializedKeysColumnBuilder;
    fn keys_column_builder(&self, capacity: usize) -> Self::ColumnBuilder {
        SerializedKeysColumnBuilder {
            inner_builder: MutableStringColumn::with_capacity(capacity),
        }
    }

    type KeysColumnIter = SerializedKeysColumnIter;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        SerializedKeysColumnIter::create(Series::check_get::<StringColumn>(column)?)
    }

    type GroupColumnsBuilder = SingleStringGroupColumnsBuilder;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> Self::GroupColumnsBuilder {
        SingleStringGroupColumnsBuilder::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodSerializer> for HashMethodSerializer {
    type State = SerializedKeysAggregatorState;
    fn aggregate_state(&self) -> Self::State {
        SerializedKeysAggregatorState {
            keys_area: Bump::new(),
            state_area: Bump::new(),
            data_state_map: HashMapKind::create_hash_table(),
            two_level_flag: false,
        }
    }

    type ColumnBuilder = SerializedKeysColumnBuilder;
    fn keys_column_builder(&self, capacity: usize) -> Self::ColumnBuilder {
        SerializedKeysColumnBuilder {
            inner_builder: MutableStringColumn::with_capacity(capacity),
        }
    }

    type KeysColumnIter = SerializedKeysColumnIter;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        SerializedKeysColumnIter::create(Series::check_get::<StringColumn>(column)?)
    }

    type GroupColumnsBuilder = SerializedKeysGroupColumnsBuilder;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> Self::GroupColumnsBuilder {
        SerializedKeysGroupColumnsBuilder::create(capacity, params)
    }
}
