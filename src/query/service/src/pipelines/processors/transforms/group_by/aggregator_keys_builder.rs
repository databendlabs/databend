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

use common_arrow::arrow::buffer::Buffer;
use common_expression::types::decimal::Decimal;
use common_expression::types::number::Number;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::Column;
use ethnum::i256;

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
    pub _t: PhantomData<&'a ()>,
    pub values: Vec<T>,
}

impl<'a, T: LargeNumber> KeysColumnBuilder for LargeFixedKeysColumnBuilder<'a, T> {
    type T = &'a T;

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
