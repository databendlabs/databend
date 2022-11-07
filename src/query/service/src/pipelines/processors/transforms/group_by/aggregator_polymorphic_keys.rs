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
use common_datablocks::HashMethodKeysU256;
use common_datablocks::HashMethodKeysU512;
use common_datablocks::HashMethodSerializer;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashtableLike;
use common_hashtable::UnsizedHashMap;
use primitive_types::U256;
use primitive_types::U512;

use super::aggregator_keys_builder::LargeFixedKeysColumnBuilder;
use super::aggregator_keys_iter::LargeFixedKeysColumnIter;
use crate::pipelines::processors::transforms::group_by::aggregator_groups_builder::FixedKeysGroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::aggregator_groups_builder::GroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::aggregator_groups_builder::SerializedKeysGroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::aggregator_keys_builder::FixedKeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::aggregator_keys_builder::KeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::aggregator_keys_builder::SerializedKeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::aggregator_keys_iter::FixedKeysColumnIter;
use crate::pipelines::processors::transforms::group_by::aggregator_keys_iter::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::aggregator_keys_iter::SerializedKeysColumnIter;
use crate::pipelines::processors::AggregatorParams;

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
// use databend_query::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
// use databend_query::pipelines::processors::transforms::group_by::aggregator_state::SerializedKeysAggregatorState;
// use databend_query::pipelines::processors::transforms::group_by::aggregator_keys_builder::SerializedKeysColumnBuilder;
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
    type HashTable: HashtableLike<Key = Method::HashKey, Value = usize> + Send;
    fn create_hash_table(&self) -> Self::HashTable;

    #[inline(always)]
    fn cast_key_ref<'a>(
        key: Method::HashKeyRef<'a>,
    ) -> <Self::HashTable as HashtableLike>::KeyRef<'a>;

    type ColumnBuilder<'a>: KeysColumnBuilder<T = <Self::HashTable as HashtableLike>::KeyRef<'a>>
    where
        Self: 'a,
        Method: 'a;

    fn keys_column_builder(&self, capacity: usize) -> Self::ColumnBuilder<'_>;

    type KeysColumnIter<'a>: KeysColumnIter<<Self::HashTable as HashtableLike>::KeyRef<'a>>
    where
        Self: 'a,
        Method: 'a;

    fn keys_iter_from_column<'a>(&self, column: &'a ColumnRef) -> Result<Self::KeysColumnIter<'a>>;

    type GroupColumnsBuilder<'a>: GroupColumnsBuilder<
        T = <Self::HashTable as HashtableLike>::KeyRef<'a>,
    >
    where
        Self: 'a,
        Method: 'a;

    fn group_columns_builder<'a>(
        &'a self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> Self::GroupColumnsBuilder<'a>;
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u8>> for HashMethodFixedKeys<u8> {
    type HashTable = HashMap<u8, usize>;

    fn create_hash_table(&self) -> Self::HashTable {
        HashMap::new()
    }

    #[inline(always)]
    fn cast_key_ref<'a>(key: u8) -> u8
    where Self: 'a {
        key
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<u8>;
    fn keys_column_builder(&self, capacity: usize) -> FixedKeysColumnBuilder<u8> {
        FixedKeysColumnBuilder::<u8> {
            inner_builder: MutablePrimitiveColumn::<u8>::with_capacity(capacity),
        }
    }
    type KeysColumnIter<'a> = FixedKeysColumnIter<u8>;
    fn keys_iter_from_column<'a>(&self, column: &'a ColumnRef) -> Result<Self::KeysColumnIter<'a>> {
        FixedKeysColumnIter::create(Series::check_get::<PrimitiveColumn<u8>>(column)?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<u8>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u8> {
        FixedKeysGroupColumnsBuilder::<u8>::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u16>> for HashMethodFixedKeys<u16> {
    type HashTable = HashMap<u16, usize>;

    fn create_hash_table(&self) -> Self::HashTable {
        HashMap::new()
    }

    #[inline(always)]
    fn cast_key_ref<'a>(key: u16) -> u16
    where Self: 'a {
        key
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<u16>;
    fn keys_column_builder(&self, capacity: usize) -> FixedKeysColumnBuilder<u16> {
        FixedKeysColumnBuilder::<u16> {
            inner_builder: MutablePrimitiveColumn::<u16>::with_capacity(capacity),
        }
    }
    type KeysColumnIter<'a> = FixedKeysColumnIter<u16>;
    fn keys_iter_from_column<'a>(&self, column: &'a ColumnRef) -> Result<Self::KeysColumnIter<'a>> {
        FixedKeysColumnIter::create(Series::check_get::<PrimitiveColumn<u16>>(column)?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<u16>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u16> {
        FixedKeysGroupColumnsBuilder::<u16>::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u32>> for HashMethodFixedKeys<u32> {
    type HashTable = HashMap<u32, usize>;

    fn create_hash_table(&self) -> Self::HashTable {
        HashMap::new()
    }

    #[inline(always)]
    fn cast_key_ref<'a>(key: u32) -> u32
    where Self: 'a {
        key
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<u32>;
    fn keys_column_builder(&self, capacity: usize) -> FixedKeysColumnBuilder<u32> {
        FixedKeysColumnBuilder::<u32> {
            inner_builder: MutablePrimitiveColumn::<u32>::with_capacity(capacity),
        }
    }
    type KeysColumnIter<'a> = FixedKeysColumnIter<u32>;
    fn keys_iter_from_column<'a>(&self, column: &'a ColumnRef) -> Result<Self::KeysColumnIter<'a>> {
        FixedKeysColumnIter::create(Series::check_get::<PrimitiveColumn<u32>>(column)?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<u32>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u32> {
        FixedKeysGroupColumnsBuilder::<u32>::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u64>> for HashMethodFixedKeys<u64> {
    type HashTable = HashMap<u64, usize>;

    fn create_hash_table(&self) -> Self::HashTable {
        HashMap::new()
    }

    #[inline(always)]
    fn cast_key_ref<'a>(key: u64) -> u64
    where Self: 'a {
        key
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<u64>;
    fn keys_column_builder(&self, capacity: usize) -> FixedKeysColumnBuilder<u64> {
        FixedKeysColumnBuilder::<u64> {
            inner_builder: MutablePrimitiveColumn::<u64>::with_capacity(capacity),
        }
    }
    type KeysColumnIter<'a> = FixedKeysColumnIter<u64>;
    fn keys_iter_from_column<'a>(&self, column: &'a ColumnRef) -> Result<Self::KeysColumnIter<'a>> {
        FixedKeysColumnIter::create(Series::check_get::<PrimitiveColumn<u64>>(column)?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<u64>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u64> {
        FixedKeysGroupColumnsBuilder::<u64>::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU128> for HashMethodKeysU128 {
    type HashTable = HashMap<u128, usize>;

    fn create_hash_table(&self) -> Self::HashTable {
        HashMap::new()
    }

    #[inline(always)]
    fn cast_key_ref<'a>(key: u128) -> u128
    where Self: 'a {
        key
    }

    type ColumnBuilder<'a> = LargeFixedKeysColumnBuilder<u128>;
    fn keys_column_builder(&self, capacity: usize) -> LargeFixedKeysColumnBuilder<u128> {
        LargeFixedKeysColumnBuilder {
            inner_builder: MutableStringColumn::with_capacity(capacity * 16),
            _t: PhantomData,
        }
    }

    type KeysColumnIter<'a> = LargeFixedKeysColumnIter<u128>;
    fn keys_iter_from_column<'a>(&self, column: &'a ColumnRef) -> Result<Self::KeysColumnIter<'a>> {
        LargeFixedKeysColumnIter::create(Series::check_get::<StringColumn>(column)?)
    }

    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<u128>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u128> {
        FixedKeysGroupColumnsBuilder::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU256> for HashMethodKeysU256 {
    type HashTable = HashMap<U256, usize>;

    fn create_hash_table(&self) -> Self::HashTable {
        HashMap::new()
    }

    #[inline(always)]
    fn cast_key_ref<'a>(key: U256) -> U256
    where Self: 'a {
        key
    }

    type ColumnBuilder<'a> = LargeFixedKeysColumnBuilder<U256>;
    fn keys_column_builder(&self, capacity: usize) -> LargeFixedKeysColumnBuilder<U256> {
        LargeFixedKeysColumnBuilder {
            inner_builder: MutableStringColumn::with_capacity(capacity * 32),
            _t: PhantomData,
        }
    }

    type KeysColumnIter<'a> = LargeFixedKeysColumnIter<U256>;
    fn keys_iter_from_column<'a>(&self, column: &'a ColumnRef) -> Result<Self::KeysColumnIter<'a>> {
        LargeFixedKeysColumnIter::create(Series::check_get::<StringColumn>(column)?)
    }

    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<U256>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<U256> {
        FixedKeysGroupColumnsBuilder::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU512> for HashMethodKeysU512 {
    type HashTable = HashMap<U512, usize>;

    fn create_hash_table(&self) -> Self::HashTable {
        HashMap::new()
    }

    #[inline(always)]
    fn cast_key_ref<'a>(key: U512) -> U512
    where Self: 'a {
        key
    }

    type ColumnBuilder<'a> = LargeFixedKeysColumnBuilder<U512>;
    fn keys_column_builder(&self, capacity: usize) -> LargeFixedKeysColumnBuilder<U512> {
        LargeFixedKeysColumnBuilder {
            inner_builder: MutableStringColumn::with_capacity(capacity * 64),
            _t: PhantomData,
        }
    }

    type KeysColumnIter<'a> = LargeFixedKeysColumnIter<U512>;
    fn keys_iter_from_column<'a>(&self, column: &'a ColumnRef) -> Result<Self::KeysColumnIter<'a>> {
        LargeFixedKeysColumnIter::create(Series::check_get::<StringColumn>(column)?)
    }

    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<U512>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<U512> {
        FixedKeysGroupColumnsBuilder::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodSerializer> for HashMethodSerializer {
    type HashTable = UnsizedHashMap<[u8], usize>;

    fn create_hash_table(&self) -> Self::HashTable {
        UnsizedHashMap::new()
    }

    #[inline(always)]
    fn cast_key_ref<'a>(key: &'a [u8]) -> &'a [u8]
    where Self: 'a {
        key
    }

    type ColumnBuilder<'a> = SerializedKeysColumnBuilder<'a>;
    fn keys_column_builder(&self, capacity: usize) -> SerializedKeysColumnBuilder<'_> {
        SerializedKeysColumnBuilder::create(capacity)
    }

    type KeysColumnIter<'a> = SerializedKeysColumnIter<'a>;
    fn keys_iter_from_column<'a>(&self, column: &'a ColumnRef) -> Result<Self::KeysColumnIter<'a>> {
        SerializedKeysColumnIter::create(Series::check_get::<StringColumn>(column)?)
    }

    type GroupColumnsBuilder<'a> = SerializedKeysGroupColumnsBuilder<'a>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> SerializedKeysGroupColumnsBuilder<'_> {
        SerializedKeysGroupColumnsBuilder::create(capacity, params)
    }
}
