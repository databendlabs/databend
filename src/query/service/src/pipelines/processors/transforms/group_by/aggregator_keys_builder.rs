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

use common_expression::types::number::Number;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::Column;

use super::large_number::LargeNumber;

/// Remove the group by key from the state and rebuild it into a column
pub trait KeysColumnBuilder {
    type T;

    fn append_value(&mut self, v: Self::T);
    fn finish(self) -> Column;
}

pub struct FixedKeysColumnBuilder<'a, T: Number> {
    pub _t: PhantomData<&'a ()>,
    pub inner_builder: Vec<T>,
}

impl<'a, T: Number> KeysColumnBuilder for FixedKeysColumnBuilder<'a, T> {
    type T = &'a T;

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

    fn append_value(&mut self, v: &'a [u8]) {
        self.inner_builder.put_slice(v);
        self.inner_builder.commit_row();
    }

    fn finish(self) -> Column {
        debug_assert_eq!(self._initial, self.inner_builder.data.len());
        Column::String(self.inner_builder.build())
    }
}

pub struct LargeFixedKeysColumnBuilder<'a, T: LargeNumber> {
    pub values: Vec<u8>,
    pub _t: PhantomData<&'a T>,
}

impl<'a, T: LargeNumber> KeysColumnBuilder for LargeFixedKeysColumnBuilder<'a, T> {
    type T = &'a T;

    #[inline]
    fn append_value(&mut self, v: Self::T) {
        let values = &mut self.values;
        values.reserve(T::BYTE_SIZE);
        let len = values.len();
        unsafe { values.set_len(len + T::BYTE_SIZE) }
        v.serialize_to(&mut values[len..len + T::BYTE_SIZE]);
    }

    #[inline]
    fn finish(self) -> Column {
        let len = self.values.len() / T::BYTE_SIZE;
        let offsets = (0..=len).map(|x| (x * T::BYTE_SIZE) as u64).collect();
        let builder = StringColumnBuilder::from_data(self.values, offsets);
        Column::String(builder.build())
    }
}
