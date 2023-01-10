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

use common_expression::large_number::LargeNumber;
use common_expression::types::number::Number;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::Column;

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

pub struct SerializedKeysColumnBuilder<'a> {
    pub inner_builder: StringColumnBuilder,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> SerializedKeysColumnBuilder<'a> {
    pub fn create(capacity: usize) -> Self {
        SerializedKeysColumnBuilder {
            inner_builder: StringColumnBuilder::with_capacity(capacity, capacity),
            _phantom: PhantomData,
        }
    }
}

impl<'a> KeysColumnBuilder for SerializedKeysColumnBuilder<'a> {
    type T = &'a [u8];

    fn append_value(&mut self, v: &'a [u8]) {
        self.inner_builder.put_slice(v);
        self.inner_builder.commit_row();
    }

    fn finish(self) -> Column {
        Column::String(self.inner_builder.build())
    }
}

pub struct LargeFixedKeysColumnBuilder<'a, T: LargeNumber> {
    pub inner_builder: StringColumnBuilder,
    pub _t: PhantomData<&'a T>,
}

impl<'a, T: LargeNumber> KeysColumnBuilder for LargeFixedKeysColumnBuilder<'a, T> {
    type T = &'a T;

    #[inline]
    fn append_value(&mut self, v: Self::T) {
        let values = &mut self.inner_builder.data;
        let new_len = values.len() + T::BYTE_SIZE;
        values.resize(new_len, 0);
        v.serialize_to(&mut values[new_len - T::BYTE_SIZE..]);
        self.inner_builder.commit_row();
    }

    #[inline]
    fn finish(self) -> Column {
        Column::String(self.inner_builder.build())
    }
}
