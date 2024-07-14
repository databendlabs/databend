// Copyright 2021 Datafuse Labs
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
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::number::*;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodDictionarySerializer;
use databend_common_expression::HashMethodFixedKeys;
use databend_common_expression::HashMethodKeysU128;
use databend_common_expression::HashMethodKeysU256;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_expression::InputColumns;
use databend_common_expression::KeyAccessor;
use databend_common_expression::KeysState;
use databend_common_hashtable::DictionaryKeys;
use databend_common_hashtable::DictionaryStringHashMap;
use databend_common_hashtable::FastHash;
use databend_common_hashtable::HashMap;
use databend_common_hashtable::HashtableEntryMutRefLike;
use databend_common_hashtable::HashtableEntryRefLike;
use databend_common_hashtable::HashtableLike;
use databend_common_hashtable::LookupHashMap;
use databend_common_hashtable::PartitionedHashMap;
use databend_common_hashtable::ShortStringHashMap;
use databend_common_hashtable::StringHashMap;
use ethnum::U256;
use log::info;

use super::aggregator_keys_builder::LargeFixedKeysColumnBuilder;
use super::aggregator_keys_iter::LargeFixedKeysColumnIter;
use super::BUCKETS_LG2;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::HashTableCell;
use crate::pipelines::processors::transforms::aggregator::PartitionedHashTableDropper;
use crate::pipelines::processors::transforms::group_by::Area;
use crate::pipelines::processors::transforms::group_by::ArenaHolder;
use crate::pipelines::processors::transforms::group_by::BinaryKeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::DictionaryBinaryKeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::DictionarySerializedKeysColumnIter;
use crate::pipelines::processors::transforms::group_by::DictionarySerializedKeysGroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::FixedKeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::FixedKeysColumnIter;
use crate::pipelines::processors::transforms::group_by::FixedKeysGroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::GroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::SerializedKeysColumnIter;
use crate::pipelines::processors::transforms::group_by::SerializedKeysGroupColumnsBuilder;

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
// use databend_common_expression::HashMethodSerializer;
// use databend_query::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
// use databend_query::pipelines::processors::transforms::group_by::aggregator_state::SerializedKeysAggregatorState;
// use databend_query::pipelines::processors::transforms::group_by::aggregator_keys_builder::StringKeysColumnBuilder;
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
//     type ColumnBuilder = StringKeysColumnBuilder;
//     fn state_array_builder(&self, capacity: usize) -> Self::ColumnBuilder {
//         StringKeysColumnBuilder {
//             inner_builder: MutableStringColumn::with_capacity(capacity),
//         }
//     }
// }
//
pub trait PolymorphicKeysHelper<Method: HashMethod>: Send + Sync + 'static {
    const SUPPORT_PARTITIONED: bool;

    type HashTable<T: Send + Sync + 'static>: HashtableLike<Key = Method::HashKey, Value = T>
        + Send
        + Sync
        + 'static;
    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        _bump: Arc<Bump>,
    ) -> Result<Self::HashTable<T>>;

    type ColumnBuilder<'a>: KeysColumnBuilder<T = &'a Method::HashKey>
    where
        Self: 'a,
        Method: 'a;

    fn keys_column_builder(
        &self,
        capacity: usize,
        value_capacity: usize,
    ) -> Self::ColumnBuilder<'_>;

    type KeysColumnIter: KeysColumnIter<Method::HashKey>;

    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter>;

    type GroupColumnsBuilder<'a>: GroupColumnsBuilder<T = &'a Method::HashKey>
    where
        Self: 'a,
        Method: 'a;

    fn group_columns_builder(
        &self,
        capacity: usize,
        _data_capacity: usize,
        params: &AggregatorParams,
    ) -> Self::GroupColumnsBuilder<'_>;

    fn get_hash(&self, v: &Method::HashKey) -> u64;
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u8>> for HashMethodFixedKeys<u8> {
    const SUPPORT_PARTITIONED: bool = false;

    type HashTable<T: Send + Sync + 'static> = LookupHashMap<u8, 256, T>;

    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        _bump: Arc<Bump>,
    ) -> Result<Self::HashTable<T>> {
        Ok(LookupHashMap::create(Default::default()))
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<'a, u8>;
    fn keys_column_builder(&self, capacity: usize, _: usize) -> FixedKeysColumnBuilder<u8> {
        FixedKeysColumnBuilder::<u8> {
            _t: Default::default(),
            inner_builder: Vec::with_capacity(capacity),
        }
    }
    type KeysColumnIter = FixedKeysColumnIter<u8>;
    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter> {
        FixedKeysColumnIter::create(&UInt8Type::try_downcast_column(column).ok_or_else(|| {
            ErrorCode::IllegalDataType("Illegal data type for FixedKeysColumnIter<u8>".to_string())
        })?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, u8>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        _data_capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u8> {
        FixedKeysGroupColumnsBuilder::<u8>::create(capacity, params)
    }

    fn get_hash(&self, v: &u8) -> u64 {
        v.fast_hash()
    }
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u16>> for HashMethodFixedKeys<u16> {
    const SUPPORT_PARTITIONED: bool = false;

    type HashTable<T: Send + Sync + 'static> = LookupHashMap<u16, 65536, T>;

    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        _bump: Arc<Bump>,
    ) -> Result<Self::HashTable<T>> {
        Ok(LookupHashMap::create(Default::default()))
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<'a, u16>;
    fn keys_column_builder(&self, capacity: usize, _: usize) -> FixedKeysColumnBuilder<u16> {
        FixedKeysColumnBuilder::<u16> {
            _t: Default::default(),
            inner_builder: Vec::with_capacity(capacity),
        }
    }
    type KeysColumnIter = FixedKeysColumnIter<u16>;
    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter> {
        FixedKeysColumnIter::create(&UInt16Type::try_downcast_column(column).ok_or_else(|| {
            ErrorCode::IllegalDataType("Illegal data type for FixedKeysColumnIter<u16>".to_string())
        })?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, u16>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        _data_capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u16> {
        FixedKeysGroupColumnsBuilder::<u16>::create(capacity, params)
    }

    fn get_hash(&self, v: &u16) -> u64 {
        v.fast_hash()
    }
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u32>> for HashMethodFixedKeys<u32> {
    const SUPPORT_PARTITIONED: bool = true;

    type HashTable<T: Send + Sync + 'static> = HashMap<u32, T>;

    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        _bump: Arc<Bump>,
    ) -> Result<Self::HashTable<T>> {
        Ok(HashMap::new())
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<'a, u32>;
    fn keys_column_builder(&self, capacity: usize, _: usize) -> FixedKeysColumnBuilder<u32> {
        FixedKeysColumnBuilder::<u32> {
            _t: Default::default(),
            inner_builder: Vec::with_capacity(capacity),
        }
    }
    type KeysColumnIter = FixedKeysColumnIter<u32>;
    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter> {
        FixedKeysColumnIter::create(&UInt32Type::try_downcast_column(column).ok_or_else(|| {
            ErrorCode::IllegalDataType("Illegal data type for FixedKeysColumnIter<u32>".to_string())
        })?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, u32>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        _data_capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u32> {
        FixedKeysGroupColumnsBuilder::<u32>::create(capacity, params)
    }

    fn get_hash(&self, v: &u32) -> u64 {
        v.fast_hash()
    }
}

impl PolymorphicKeysHelper<HashMethodFixedKeys<u64>> for HashMethodFixedKeys<u64> {
    const SUPPORT_PARTITIONED: bool = true;

    type HashTable<T: Send + Sync + 'static> = HashMap<u64, T>;

    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        _bump: Arc<Bump>,
    ) -> Result<Self::HashTable<T>> {
        Ok(HashMap::new())
    }

    type ColumnBuilder<'a> = FixedKeysColumnBuilder<'a, u64>;
    fn keys_column_builder(&self, capacity: usize, _: usize) -> FixedKeysColumnBuilder<u64> {
        FixedKeysColumnBuilder::<u64> {
            _t: Default::default(),
            inner_builder: Vec::with_capacity(capacity),
        }
    }
    type KeysColumnIter = FixedKeysColumnIter<u64>;
    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter> {
        FixedKeysColumnIter::create(&UInt64Type::try_downcast_column(column).ok_or_else(|| {
            ErrorCode::IllegalDataType("Illegal data type for FixedKeysColumnIter<u64>".to_string())
        })?)
    }
    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, u64>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        _data_capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u64> {
        FixedKeysGroupColumnsBuilder::<u64>::create(capacity, params)
    }

    fn get_hash(&self, v: &u64) -> u64 {
        v.fast_hash()
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU128> for HashMethodKeysU128 {
    const SUPPORT_PARTITIONED: bool = true;

    type HashTable<T: Send + Sync + 'static> = HashMap<u128, T>;

    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        _bump: Arc<Bump>,
    ) -> Result<Self::HashTable<T>> {
        Ok(HashMap::new())
    }

    type ColumnBuilder<'a> = LargeFixedKeysColumnBuilder<'a, u128>;
    fn keys_column_builder(&self, capacity: usize, _: usize) -> LargeFixedKeysColumnBuilder<u128> {
        LargeFixedKeysColumnBuilder::<u128> {
            values: Vec::with_capacity(capacity * 16),
            _t: PhantomData,
        }
    }

    type KeysColumnIter = LargeFixedKeysColumnIter<u128>;
    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter> {
        let buffer = column
            .as_decimal()
            .and_then(|c| c.as_decimal128())
            .ok_or_else(|| {
                ErrorCode::IllegalDataType(
                    "Illegal data type for LargeFixedKeysColumnIter<u128>".to_string(),
                )
            })?;
        let buffer = unsafe { std::mem::transmute(buffer.0.clone()) };
        LargeFixedKeysColumnIter::create(buffer)
    }

    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, u128>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        _data_capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<u128> {
        FixedKeysGroupColumnsBuilder::create(capacity, params)
    }

    fn get_hash(&self, v: &u128) -> u64 {
        v.fast_hash()
    }
}

impl PolymorphicKeysHelper<HashMethodKeysU256> for HashMethodKeysU256 {
    const SUPPORT_PARTITIONED: bool = true;

    type HashTable<T: Send + Sync + 'static> = HashMap<U256, T>;

    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        _bump: Arc<Bump>,
    ) -> Result<Self::HashTable<T>> {
        Ok(HashMap::new())
    }

    type ColumnBuilder<'a> = LargeFixedKeysColumnBuilder<'a, U256>;
    fn keys_column_builder(&self, capacity: usize, _: usize) -> LargeFixedKeysColumnBuilder<U256> {
        LargeFixedKeysColumnBuilder::<U256> {
            values: Vec::with_capacity(capacity * 32),
            _t: PhantomData,
        }
    }

    type KeysColumnIter = LargeFixedKeysColumnIter<U256>;
    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter> {
        let buffer = column
            .as_decimal()
            .and_then(|c| c.as_decimal256())
            .ok_or_else(|| {
                ErrorCode::IllegalDataType(
                    "Illegal data type for LargeFixedKeysColumnIter<u128>".to_string(),
                )
            })?;
        let buffer = unsafe { std::mem::transmute(buffer.0.clone()) };

        LargeFixedKeysColumnIter::create(buffer)
    }

    type GroupColumnsBuilder<'a> = FixedKeysGroupColumnsBuilder<'a, U256>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        _data_capacity: usize,
        params: &AggregatorParams,
    ) -> FixedKeysGroupColumnsBuilder<U256> {
        FixedKeysGroupColumnsBuilder::create(capacity, params)
    }

    fn get_hash(&self, v: &U256) -> u64 {
        v.fast_hash()
    }
}

impl PolymorphicKeysHelper<HashMethodSingleBinary> for HashMethodSingleBinary {
    const SUPPORT_PARTITIONED: bool = true;

    type HashTable<T: Send + Sync + 'static> = ShortStringHashMap<[u8], T>;

    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        bump: Arc<Bump>,
    ) -> Result<Self::HashTable<T>> {
        Ok(ShortStringHashMap::new(bump))
    }

    type ColumnBuilder<'a> = BinaryKeysColumnBuilder<'a>;
    fn keys_column_builder(
        &self,
        capacity: usize,
        value_capacity: usize,
    ) -> BinaryKeysColumnBuilder<'_> {
        BinaryKeysColumnBuilder::create(capacity, value_capacity)
    }

    type KeysColumnIter = SerializedKeysColumnIter;
    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter> {
        SerializedKeysColumnIter::create(column.as_binary().ok_or_else(|| {
            ErrorCode::IllegalDataType("Illegal data type for SerializedKeysColumnIter".to_string())
        })?)
    }

    type GroupColumnsBuilder<'a> = SerializedKeysGroupColumnsBuilder<'a>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        data_capacity: usize,
        params: &AggregatorParams,
    ) -> SerializedKeysGroupColumnsBuilder<'_> {
        SerializedKeysGroupColumnsBuilder::create(capacity, data_capacity, params)
    }

    fn get_hash(&self, v: &[u8]) -> u64 {
        v.fast_hash()
    }
}

impl PolymorphicKeysHelper<HashMethodSerializer> for HashMethodSerializer {
    const SUPPORT_PARTITIONED: bool = true;

    type HashTable<T: Send + Sync + 'static> = StringHashMap<[u8], T>;

    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        bump: Arc<Bump>,
    ) -> Result<Self::HashTable<T>> {
        Ok(StringHashMap::new(bump))
    }

    type ColumnBuilder<'a> = BinaryKeysColumnBuilder<'a>;
    fn keys_column_builder(
        &self,
        capacity: usize,
        value_capacity: usize,
    ) -> BinaryKeysColumnBuilder<'_> {
        BinaryKeysColumnBuilder::create(capacity, value_capacity)
    }

    type KeysColumnIter = SerializedKeysColumnIter;
    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter> {
        SerializedKeysColumnIter::create(column.as_binary().ok_or_else(|| {
            ErrorCode::IllegalDataType("Illegal data type for SerializedKeysColumnIter".to_string())
        })?)
    }

    type GroupColumnsBuilder<'a> = SerializedKeysGroupColumnsBuilder<'a>;
    fn group_columns_builder(
        &self,
        capacity: usize,
        data_capacity: usize,
        params: &AggregatorParams,
    ) -> SerializedKeysGroupColumnsBuilder<'_> {
        SerializedKeysGroupColumnsBuilder::create(capacity, data_capacity, params)
    }

    fn get_hash(&self, v: &[u8]) -> u64 {
        v.fast_hash()
    }
}

impl PolymorphicKeysHelper<HashMethodDictionarySerializer> for HashMethodDictionarySerializer {
    const SUPPORT_PARTITIONED: bool = true;

    type HashTable<T: Send + Sync + 'static> = DictionaryStringHashMap<T>;

    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        bump: Arc<Bump>,
    ) -> Result<Self::HashTable<T>> {
        Ok(DictionaryStringHashMap::new(bump, self.dict_keys))
    }

    type ColumnBuilder<'a> = DictionaryBinaryKeysColumnBuilder<'a>;

    fn keys_column_builder(
        &self,
        capacity: usize,
        value_capacity: usize,
    ) -> Self::ColumnBuilder<'_> {
        DictionaryBinaryKeysColumnBuilder::create(capacity, value_capacity)
    }

    type KeysColumnIter = DictionarySerializedKeysColumnIter;

    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter> {
        DictionarySerializedKeysColumnIter::create(
            self.dict_keys,
            column.as_binary().ok_or_else(|| {
                ErrorCode::IllegalDataType(
                    "Illegal data type for SerializedKeysColumnIter".to_string(),
                )
            })?,
        )
    }

    type GroupColumnsBuilder<'a> = DictionarySerializedKeysGroupColumnsBuilder<'a>;

    fn group_columns_builder(
        &self,
        capacity: usize,
        data_capacity: usize,
        params: &AggregatorParams,
    ) -> Self::GroupColumnsBuilder<'_> {
        DictionarySerializedKeysGroupColumnsBuilder::create(capacity, data_capacity, params)
    }

    fn get_hash(&self, v: &DictionaryKeys) -> u64 {
        v.fast_hash()
    }
}

#[derive(Clone)]
pub struct PartitionedHashMethod<Method: HashMethodBounds> {
    pub(crate) method: Method,
}

impl<Method: HashMethodBounds> PartitionedHashMethod<Method> {
    pub fn create(method: Method) -> PartitionedHashMethod<Method> {
        PartitionedHashMethod::<Method> { method }
    }

    pub fn convert_hashtable<T>(
        method: &Method,
        mut cell: HashTableCell<Method, T>,
    ) -> Result<HashTableCell<PartitionedHashMethod<Method>, T>>
    where
        T: Copy + Send + Sync + 'static,
        Self: PolymorphicKeysHelper<PartitionedHashMethod<Method>>,
    {
        let instant = Instant::now();
        let arena = Arc::new(Bump::new());
        let partitioned_method = Self::create(method.clone());
        let mut partitioned_hashtable = partitioned_method.create_hash_table(arena)?;

        unsafe {
            for item in cell.hashtable.iter() {
                match partitioned_hashtable.insert_and_entry(item.key()) {
                    Ok(mut entry) => {
                        *entry.get_mut() = *item.get();
                    }
                    Err(mut entry) => {
                        *entry.get_mut() = *item.get();
                    }
                };
            }
        }

        info!(
            "Convert to Partitioned HashTable elapsed: {:?}",
            instant.elapsed()
        );

        let arena = std::mem::replace(&mut cell.arena, Area::create());
        cell.arena_holders.push(ArenaHolder::create(Some(arena)));
        let arena_holders = cell.arena_holders.to_vec();

        let _old_dropper = cell._dropper.clone().unwrap();
        let _new_dropper = PartitionedHashTableDropper::<Method, T>::create(_old_dropper);

        // TODO(winter): No idea(may memory leak).
        // We need to ensure that the following two lines of code are atomic.
        // take_old_dropper before create new HashTableCell - may memory leak
        // create new HashTableCell before take_old_dropper - may double free memory
        let _old_dropper = cell._dropper.take();
        let mut cell = HashTableCell::create(partitioned_hashtable, _new_dropper);
        cell.arena_holders = arena_holders;
        Ok(cell)
    }
}

impl<Method: HashMethodBounds> HashMethod for PartitionedHashMethod<Method> {
    type HashKey = Method::HashKey;
    type HashKeyIter<'a> = Method::HashKeyIter<'a> where Self: 'a;

    fn name(&self) -> String {
        format!("Partitioned{}", self.method.name())
    }

    fn build_keys_state(
        &self,
        group_columns: (InputColumns, &[DataType]),
        rows: usize,
    ) -> Result<KeysState> {
        self.method.build_keys_state(group_columns, rows)
    }

    fn build_keys_iter<'a>(&self, keys_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        self.method.build_keys_iter(keys_state)
    }

    fn build_keys_accessor_and_hashes(
        &self,
        keys_state: KeysState,
        hashes: &mut Vec<u64>,
    ) -> Result<Box<dyn KeyAccessor<Key = Self::HashKey>>> {
        self.method
            .build_keys_accessor_and_hashes(keys_state, hashes)
    }
}

impl<Method> PolymorphicKeysHelper<PartitionedHashMethod<Method>> for PartitionedHashMethod<Method>
where
    Self: HashMethod<HashKey = Method::HashKey>,
    Method: HashMethod + PolymorphicKeysHelper<Method>,
{
    // Partitioned cannot be recursive
    const SUPPORT_PARTITIONED: bool = false;

    type HashTable<T: Send + Sync + 'static> =
        PartitionedHashMap<Method::HashTable<T>, BUCKETS_LG2>;

    fn create_hash_table<T: Send + Sync + 'static>(
        &self,
        arena: Arc<Bump>,
    ) -> Result<Self::HashTable<T>> {
        let buckets = (1 << BUCKETS_LG2) as usize;
        let mut tables = Vec::with_capacity(buckets);

        for _index in 0..buckets {
            tables.push(self.method.create_hash_table(arena.clone())?);
        }

        Ok(PartitionedHashMap::<_, BUCKETS_LG2>::create(arena, tables))
    }

    type ColumnBuilder<'a> = Method::ColumnBuilder<'a> where Self: 'a, PartitionedHashMethod<Method>: 'a;

    fn keys_column_builder(
        &self,
        capacity: usize,
        value_capacity: usize,
    ) -> Self::ColumnBuilder<'_> {
        self.method.keys_column_builder(capacity, value_capacity)
    }

    type KeysColumnIter = Method::KeysColumnIter;

    fn keys_iter_from_column(&self, column: &Column) -> Result<Self::KeysColumnIter> {
        self.method.keys_iter_from_column(column)
    }

    type GroupColumnsBuilder<'a> = Method::GroupColumnsBuilder<'a> where Self: 'a, PartitionedHashMethod<Method>: 'a;

    fn group_columns_builder(
        &self,
        capacity: usize,
        data_capacity: usize,
        params: &AggregatorParams,
    ) -> Self::GroupColumnsBuilder<'_> {
        self.method
            .group_columns_builder(capacity, data_capacity, params)
    }

    fn get_hash(&self, v: &Method::HashKey) -> u64 {
        self.method.get_hash(v)
    }
}

pub trait HashMethodBounds: HashMethod + PolymorphicKeysHelper<Self> {}

impl<T: HashMethod + PolymorphicKeysHelper<T>> HashMethodBounds for T {}
