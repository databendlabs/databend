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

use std::hash::Hash;
use std::marker::PhantomData;

use common_arrow::arrow::buffer::Buffer;
use common_expression::types::decimal::Decimal;
use common_expression::types::number::Number;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::Column;
use common_hashtable::DictionaryKeys;
use common_hashtable::DictionaryStringHashMap;
use common_hashtable::FastHash;
use common_hashtable::HashtableLike;
use common_hashtable::LookupHashMap;
use common_hashtable::PartitionedHashMap;
use ethnum::i256;

use super::large_number::LargeNumber;

/// Remove the group by key from the state and rebuild it into a column
pub trait KeysColumnBuilder {
    type T;
    type HashTable: HashtableLike;

    fn append_value(&mut self, v: Self::T);
    fn finish(self, hashtable: &Self::HashTable) -> Column;
}

pub struct FixedKeysColumnBuilder<'a, T: Number, H: HashtableLike> {
    pub _t: PhantomData<&'a H>,
    pub inner_builder: Vec<T>,
}

impl<'a, T: Number, H: HashtableLike> KeysColumnBuilder for FixedKeysColumnBuilder<'a, T, H> {
    type T = &'a T;
    type HashTable = H;

    #[inline]
    fn append_value(&mut self, v: Self::T) {
        self.inner_builder.push(*v)
    }

    #[inline]
    fn finish(self, _: &Self::HashTable) -> Column {
        NumberType::<T>::upcast_column(NumberType::<T>::build_column(self.inner_builder))
    }
}

pub struct StringKeysColumnBuilder<'a, H: HashtableLike> {
    pub inner_builder: StringColumnBuilder,

    _initial: usize,

    _phantom: PhantomData<&'a H>,
}

impl<'a, H: HashtableLike> StringKeysColumnBuilder<'a, H> {
    pub fn create(capacity: usize, value_capacity: usize) -> Self {
        StringKeysColumnBuilder {
            inner_builder: StringColumnBuilder::with_capacity(capacity, value_capacity),
            _phantom: PhantomData,
            _initial: value_capacity,
        }
    }
}

impl<'a, H: HashtableLike> KeysColumnBuilder for StringKeysColumnBuilder<'a, H> {
    type T = &'a [u8];
    type HashTable = H;

    fn append_value(&mut self, v: &'a [u8]) {
        self.inner_builder.put_slice(v);
        self.inner_builder.commit_row();
    }

    fn finish(self, _: &Self::HashTable) -> Column {
        debug_assert_eq!(self._initial, self.inner_builder.data.len());
        Column::String(self.inner_builder.build())
    }
}

pub struct LargeFixedKeysColumnBuilder<'a, T: LargeNumber, H: HashtableLike> {
    pub _t: PhantomData<&'a H>,
    pub values: Vec<T>,
}

impl<'a, T: LargeNumber, H: HashtableLike> KeysColumnBuilder
    for LargeFixedKeysColumnBuilder<'a, T, H>
{
    type T = &'a T;
    type HashTable = H;

    #[inline]
    fn append_value(&mut self, v: Self::T) {
        self.values.push(*v);
    }

    #[inline]
    fn finish(self, _: &Self::HashTable) -> Column {
        match T::BYTE_SIZE {
            16 => {
                let values: Buffer<T> = self.values.into();
                let values: Buffer<i128> = unsafe { std::mem::transmute(values) };
                let col = i128::to_column_from_buffer(values, i128::default_decimal_size());
                Column::Decimal(col)
            }
            32 => {
                let values: Buffer<T> = self.values.into();
                let values: Buffer<i256> = unsafe { std::mem::transmute(values) };
                let col = i256::to_column_from_buffer(values, i256::default_decimal_size());
                Column::Decimal(col)
            }
            _ => unreachable!(),
        }
    }
}

pub struct DictionaryStringKeysColumnBuilder<'a, V: Send + Sync + 'static> {
    pub inner_builder: StringColumnBuilder,
    _phantom: PhantomData<&'a V>,
}

impl<'a, V: Send + Sync + 'static> DictionaryStringKeysColumnBuilder<'a, V> {
    pub fn create(capacity: usize, value_capacity: usize) -> Self {
        DictionaryStringKeysColumnBuilder {
            inner_builder: StringColumnBuilder::with_capacity(capacity, value_capacity),
            _phantom: PhantomData,
        }
    }
}

impl<'a, V: Send + Sync + 'static> KeysColumnBuilder for DictionaryStringKeysColumnBuilder<'a, V> {
    type T = &'a DictionaryKeys;
    type HashTable = DictionaryStringHashMap<V>;

    #[inline(always)]
    fn append_value(&mut self, _: &'a DictionaryKeys) {}

    #[inline(always)]
    fn finish(self, hashtable: &Self::HashTable) -> Column {
        let dict_len = hashtable.dictionary_hashset.len();
        let dict_unsize_key = hashtable
            .dictionary_hashset
            .unsize_key_size()
            .unwrap_or_default();

        let mut column = StringColumnBuilder::with_capacity(dict_len, dict_unsize_key);
        for entry in hashtable.dictionary_hashset.iter() {
            // hashtable.dictionary_hashset.insert_and_entry()
            column.put(entry.key())
            // column.put
            // TODO: serialize
        }
        unimplemented!()
    }
}

pub struct PartitionedKeysBuilder<I: KeysColumnBuilder>
where <<I as KeysColumnBuilder>::HashTable as HashtableLike>::Key: FastHash
{
    pub inner: I,
}

impl<I: KeysColumnBuilder> PartitionedKeysBuilder<I>
where <<I as KeysColumnBuilder>::HashTable as HashtableLike>::Key: FastHash
{
    pub fn create(inner: I) -> Self {
        PartitionedKeysBuilder { inner }
    }
}

impl<I: KeysColumnBuilder> KeysColumnBuilder for PartitionedKeysBuilder<I>
where <<I as KeysColumnBuilder>::HashTable as HashtableLike>::Key: FastHash
{
    type T = I::T;
    type HashTable = PartitionedHashMap<I::HashTable, 8>;

    fn append_value(&mut self, _: Self::T) {
        unimplemented!()
    }

    fn finish(self, _: &Self::HashTable) -> Column {
        unimplemented!()
    }
}
