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

use byteorder::BigEndian;
use byteorder::WriteBytesExt;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::number::Number;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;
use databend_common_hashtable::DictionaryKeys;
use ethnum::i256;

use super::large_number::LargeNumber;

/// Remove the group by key from the state and rebuild it into a column
pub trait KeysColumnBuilder {
    type T;

    fn bytes_size(&self) -> usize;
    fn append_value(&mut self, v: Self::T);
    fn finish(self) -> Column;
}

pub struct FixedKeysColumnBuilder<'a, T: Number> {
    pub _t: PhantomData<&'a ()>,
    pub inner_builder: Vec<T>,
}

impl<'a, T: Number> KeysColumnBuilder for FixedKeysColumnBuilder<'a, T> {
    type T = &'a T;

    fn bytes_size(&self) -> usize {
        self.inner_builder.len() * std::mem::size_of::<T>()
    }

    #[inline]
    fn append_value(&mut self, v: Self::T) {
        self.inner_builder.push(*v)
    }

    #[inline]
    fn finish(self) -> Column {
        NumberType::<T>::upcast_column(NumberType::<T>::build_column(self.inner_builder))
    }
}

pub struct StringKeysColumnBuilder<'a> {
    pub inner_builder: StringColumnBuilder,

    _initial: usize,

    _phantom: PhantomData<&'a ()>,
}

impl<'a> StringKeysColumnBuilder<'a> {
    pub fn create(capacity: usize, value_capacity: usize) -> Self {
        StringKeysColumnBuilder {
            inner_builder: StringColumnBuilder::with_capacity(capacity, value_capacity),
            _phantom: PhantomData,
            _initial: value_capacity,
        }
    }
}

impl<'a> KeysColumnBuilder for StringKeysColumnBuilder<'a> {
    type T = &'a [u8];

    fn bytes_size(&self) -> usize {
        self.inner_builder.data.len()
    }

    fn append_value(&mut self, v: &'a [u8]) {
        self.inner_builder.put_slice(v);
        self.inner_builder.commit_row();
    }

    fn finish(self) -> Column {
        Column::String(self.inner_builder.build())
    }
}

pub struct LargeFixedKeysColumnBuilder<'a, T: LargeNumber> {
    pub _t: PhantomData<&'a ()>,
    pub values: Vec<T>,
}

impl<'a, T: LargeNumber> KeysColumnBuilder for LargeFixedKeysColumnBuilder<'a, T> {
    type T = &'a T;

    fn bytes_size(&self) -> usize {
        self.values.len() * std::mem::size_of::<T>()
    }

    #[inline]
    fn append_value(&mut self, v: Self::T) {
        self.values.push(*v);
    }

    #[inline]
    fn finish(self) -> Column {
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

pub struct DictionaryStringKeysColumnBuilder<'a> {
    bytes_size: usize,
    data: Vec<DictionaryKeys>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> DictionaryStringKeysColumnBuilder<'a> {
    pub fn create(_: usize, _: usize) -> Self {
        DictionaryStringKeysColumnBuilder {
            bytes_size: 0,
            data: vec![],
            _phantom: PhantomData,
        }
    }
}

impl<'a> KeysColumnBuilder for DictionaryStringKeysColumnBuilder<'a> {
    type T = &'a DictionaryKeys;

    fn bytes_size(&self) -> usize {
        self.bytes_size
    }

    #[inline(always)]
    fn append_value(&mut self, v: &'a DictionaryKeys) {
        unsafe {
            for x in v.keys.as_ref() {
                self.bytes_size += x.as_ref().len() + std::mem::size_of::<u64>();
            }
        }

        self.data.push(*v)
    }

    #[inline(always)]
    fn finish(self) -> Column {
        let mut builder = StringColumnBuilder::with_capacity(self.data.len(), self.bytes_size);

        unsafe {
            for dictionary_keys in self.data {
                for dictionary_key in dictionary_keys.keys.as_ref() {
                    let i = dictionary_key.as_ref().len() as u64;
                    builder.data.write_u64::<BigEndian>(i).unwrap();
                    builder.put(dictionary_key.as_ref());
                }

                builder.commit_row();
            }
        }

        Column::String(builder.build())
    }
}
