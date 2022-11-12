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

use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datablocks::HashMethodKeysU128;
use common_datablocks::HashMethodKeysU256;
use common_datablocks::HashMethodKeysU512;
use common_datablocks::HashMethodSerializer;
use common_datablocks::KeysState;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_hashtable::FastHash;
use common_hashtable::HashMap;
use common_hashtable::HashtableLike;
use common_hashtable::LookupHashMap;
use common_hashtable::TwoLevelHashMap;
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
    const SUPPORT_TWO_LEVEL: bool;

    type HashTable: HashtableLike<Key = Method::HashKey, Value = usize> + Send;
    fn create_hash_table(&self) -> Result<Self::HashTable>;

    type ColumnBuilder<'a>: KeysColumnBuilder<T = &'a Method::HashKey>
    where
        Self: 'a,
        Method: 'a;

    fn keys_column_builder(&self, capacity: usize) -> Self::ColumnBuilder<'_>;

    type KeysColumnIter: KeysColumnIter<Method::HashKey>;

    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter>;

    type GroupColumnsBuilder<'a>: GroupColumnsBuilder<T = &'a Method::HashKey>
    where
        Self: 'a,
        Method: 'a;

    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> Self::GroupColumnsBuilder<'_>;
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u8>> for HashMethodFixedKeys<u8> {
    const SUPPORT_TWO_LEVEL: bool = false;

    type HashTable = LookupHashMap<u8, 256, usize>;

    fn create_hash_table(&self) -> Result<Self::HashTable> {
        Ok(LookupHashMap::create(Default::default()))
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<'a, u8>;
    fn keys_column_builder(&self, capacity: usize) -> FixedKeysColumnBuilder<u8> {
        FixedKeysColumnBuilder::<u8> {
            _t: Default::default(),
            inner_builder: MutablePrimitiveColumn::<u8>::with_capacity(capacity),
        }
    }
    type KeysColumnIter = FixedKeysColumnIter<u8>;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        FixedKeysColumnIter::create(Series::check_get::<PrimitiveColumn<u8>>(column)?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, u8>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u8> {
        FixedKeysGroupColumnsBuilder::<u8>::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u16>> for HashMethodFixedKeys<u16> {
    const SUPPORT_TWO_LEVEL: bool = false;

    type HashTable = LookupHashMap<u16, 65536, usize>;

    fn create_hash_table(&self) -> Result<Self::HashTable> {
        Ok(LookupHashMap::create(Default::default()))
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<'a, u16>;
    fn keys_column_builder(&self, capacity: usize) -> FixedKeysColumnBuilder<u16> {
        FixedKeysColumnBuilder::<u16> {
            _t: Default::default(),
            inner_builder: MutablePrimitiveColumn::<u16>::with_capacity(capacity),
        }
    }
    type KeysColumnIter = FixedKeysColumnIter<u16>;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        FixedKeysColumnIter::create(Series::check_get::<PrimitiveColumn<u16>>(column)?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, u16>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u16> {
        FixedKeysGroupColumnsBuilder::<u16>::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u32>> for HashMethodFixedKeys<u32> {
    const SUPPORT_TWO_LEVEL: bool = true;

    type HashTable = HashMap<u32, usize>;

    fn create_hash_table(&self) -> Result<Self::HashTable> {
        Ok(HashMap::new())
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<'a, u32>;
    fn keys_column_builder(&self, capacity: usize) -> FixedKeysColumnBuilder<u32> {
        FixedKeysColumnBuilder::<u32> {
            _t: Default::default(),
            inner_builder: MutablePrimitiveColumn::<u32>::with_capacity(capacity),
        }
    }
    type KeysColumnIter = FixedKeysColumnIter<u32>;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        FixedKeysColumnIter::create(Series::check_get::<PrimitiveColumn<u32>>(column)?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, u32>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u32> {
        FixedKeysGroupColumnsBuilder::<u32>::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u64>> for HashMethodFixedKeys<u64> {
    const SUPPORT_TWO_LEVEL: bool = true;

    type HashTable = HashMap<u64, usize>;

    fn create_hash_table(&self) -> Result<Self::HashTable> {
        Ok(HashMap::new())
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<'a, u64>;
    fn keys_column_builder(&self, capacity: usize) -> FixedKeysColumnBuilder<u64> {
        FixedKeysColumnBuilder::<u64> {
            _t: Default::default(),
            inner_builder: MutablePrimitiveColumn::<u64>::with_capacity(capacity),
        }
    }
    type KeysColumnIter = FixedKeysColumnIter<u64>;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        FixedKeysColumnIter::create(Series::check_get::<PrimitiveColumn<u64>>(column)?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, u64>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u64> {
        FixedKeysGroupColumnsBuilder::<u64>::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU128> for HashMethodKeysU128 {
    const SUPPORT_TWO_LEVEL: bool = true;

    type HashTable = HashMap<u128, usize>;

    fn create_hash_table(&self) -> Result<Self::HashTable> {
        Ok(HashMap::new())
    }

    type ColumnBuilder<'a> = LargeFixedKeysColumnBuilder<'a, u128>;
    fn keys_column_builder(&self, capacity: usize) -> LargeFixedKeysColumnBuilder<u128> {
        LargeFixedKeysColumnBuilder {
            inner_builder: MutableStringColumn::with_capacity(capacity * 16),
            _t: PhantomData,
        }
    }

    type KeysColumnIter = LargeFixedKeysColumnIter<u128>;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        LargeFixedKeysColumnIter::create(Series::check_get::<StringColumn>(column)?)
    }

    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, u128>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u128> {
        FixedKeysGroupColumnsBuilder::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU256> for HashMethodKeysU256 {
    const SUPPORT_TWO_LEVEL: bool = true;

    type HashTable = HashMap<U256, usize>;

    fn create_hash_table(&self) -> Result<Self::HashTable> {
        Ok(HashMap::new())
    }

    type ColumnBuilder<'a> = LargeFixedKeysColumnBuilder<'a, U256>;
    fn keys_column_builder(&self, capacity: usize) -> LargeFixedKeysColumnBuilder<U256> {
        LargeFixedKeysColumnBuilder {
            inner_builder: MutableStringColumn::with_capacity(capacity * 32),
            _t: PhantomData,
        }
    }

    type KeysColumnIter = LargeFixedKeysColumnIter<U256>;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        LargeFixedKeysColumnIter::create(Series::check_get::<StringColumn>(column)?)
    }

    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, U256>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<U256> {
        FixedKeysGroupColumnsBuilder::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU512> for HashMethodKeysU512 {
    const SUPPORT_TWO_LEVEL: bool = true;

    type HashTable = HashMap<U512, usize>;

    fn create_hash_table(&self) -> Result<Self::HashTable> {
        Ok(HashMap::new())
    }

    type ColumnBuilder<'a> = LargeFixedKeysColumnBuilder<'a, U512>;
    fn keys_column_builder(&self, capacity: usize) -> LargeFixedKeysColumnBuilder<U512> {
        LargeFixedKeysColumnBuilder {
            _t: PhantomData::default(),
            inner_builder: MutableStringColumn::with_capacity(capacity * 64),
        }
    }

    type KeysColumnIter = LargeFixedKeysColumnIter<U512>;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        LargeFixedKeysColumnIter::create(Series::check_get::<StringColumn>(column)?)
    }

    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, U512>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<U512> {
        FixedKeysGroupColumnsBuilder::create(capacity, params)
    }
}

impl PolymorphicKeysHelper<HashMethodSerializer> for HashMethodSerializer {
    const SUPPORT_TWO_LEVEL: bool = true;

    type HashTable = UnsizedHashMap<[u8], usize>;

    fn create_hash_table(&self) -> Result<Self::HashTable> {
        Ok(UnsizedHashMap::new())
    }

    type ColumnBuilder<'a> = SerializedKeysColumnBuilder<'a>;
    fn keys_column_builder(&self, capacity: usize) -> SerializedKeysColumnBuilder<'_> {
        SerializedKeysColumnBuilder::create(capacity)
    }

    type KeysColumnIter = SerializedKeysColumnIter;
    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
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

#[derive(Clone)]
pub struct TwoLevelHashMethod<Method: HashMethod + Send> {
    method: Method,
}

impl<Method: HashMethod + Send> TwoLevelHashMethod<Method> {
    pub fn create(method: Method) -> TwoLevelHashMethod<Method> {
        TwoLevelHashMethod::<Method> { method }
    }
}

impl<Method: HashMethod + Send> HashMethod for TwoLevelHashMethod<Method> {
    type HashKey = Method::HashKey;
    type HashKeyIter<'a> = Method::HashKeyIter<'a> where Self: 'a;

    fn name(&self) -> String {
        format!("TwoLevel{}", self.method.name())
    }

    fn build_keys_state(&self, group_columns: &[&ColumnRef], rows: usize) -> Result<KeysState> {
        self.method.build_keys_state(group_columns, rows)
    }

    fn build_keys_iter<'a>(&self, keys_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        self.method.build_keys_iter(keys_state)
    }
}

impl<Method> PolymorphicKeysHelper<TwoLevelHashMethod<Method>> for TwoLevelHashMethod<Method>
where
    Self: HashMethod<HashKey = Method::HashKey>,
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    // Two level cannot be recursive
    const SUPPORT_TWO_LEVEL: bool = false;

    type HashTable = TwoLevelHashMap<Method::HashTable>;

    fn create_hash_table(&self) -> Result<Self::HashTable> {
        let mut tables = Vec::with_capacity(256);

        for _index in 0..256 {
            tables.push(self.method.create_hash_table()?);
        }

        Ok(TwoLevelHashMap::create(tables))
    }

    type ColumnBuilder<'a> = Method::ColumnBuilder<'a> where Self: 'a, TwoLevelHashMethod<Method>: 'a;

    fn keys_column_builder(&self, capacity: usize) -> Self::ColumnBuilder<'_> {
        self.method.keys_column_builder(capacity)
    }

    type KeysColumnIter = Method::KeysColumnIter;

    fn keys_iter_from_column(&self, column: &ColumnRef) -> Result<Self::KeysColumnIter> {
        self.method.keys_iter_from_column(column)
    }

    type GroupColumnsBuilder<'a> = Method::GroupColumnsBuilder<'a> where Self: 'a, TwoLevelHashMethod<Method>: 'a;

    fn group_columns_builder(
        &self,
        capacity: usize,
        params: &AggregatorParams,
    ) -> Self::GroupColumnsBuilder<'_> {
        self.method.group_columns_builder(capacity, params)
    }
}
