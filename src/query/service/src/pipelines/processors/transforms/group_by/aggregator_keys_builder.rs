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

use common_datavalues::prelude::*;

/// Remove the group by key from the state and rebuild it into a column
pub trait KeysColumnBuilder {
    type T;

    fn append_value(&mut self, v: Self::T);
    fn finish(self) -> ColumnRef;
}

pub struct FixedKeysColumnBuilder<'a, T: PrimitiveType> {
    pub _t: PhantomData<&'a ()>,
    pub inner_builder: MutablePrimitiveColumn<T>,
}

impl<'a, T: PrimitiveType> KeysColumnBuilder for FixedKeysColumnBuilder<'a, T> {
    type T = &'a T;

    #[inline]
    fn append_value(&mut self, v: Self::T) {
        self.inner_builder.append_value(*v)
    }

    #[inline]
    fn finish(mut self) -> ColumnRef {
        self.inner_builder.to_column()
    }
}

pub struct SerializedKeysColumnBuilder<'a> {
    pub inner_builder: MutableStringColumn,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> SerializedKeysColumnBuilder<'a> {
    pub fn create(capacity: usize) -> Self {
        SerializedKeysColumnBuilder {
            inner_builder: MutableStringColumn::with_capacity(capacity),
            _phantom: PhantomData,
        }
    }
}

impl<'a> KeysColumnBuilder for SerializedKeysColumnBuilder<'a> {
    type T = &'a [u8];

    fn append_value(&mut self, v: &'a [u8]) {
        self.inner_builder.append_value(v);
    }

    fn finish(mut self) -> ColumnRef {
        self.inner_builder.to_column()
    }
}

pub struct LargeFixedKeysColumnBuilder<'a, T: LargePrimitive> {
    pub inner_builder: MutableStringColumn,
    pub _t: PhantomData<&'a T>,
}

impl<'a, T: LargePrimitive> KeysColumnBuilder for LargeFixedKeysColumnBuilder<'a, T> {
    type T = &'a T;

    #[inline]
    fn append_value(&mut self, v: Self::T) {
        let values = self.inner_builder.values_mut();
        let new_len = values.len() + T::BYTE_SIZE;
        values.resize(new_len, 0);
        v.serialize_to(&mut values[new_len - T::BYTE_SIZE..]);
        self.inner_builder.offsets_mut().push(new_len as i64);
    }

    #[inline]
    fn finish(mut self) -> ColumnRef {
        self.inner_builder.to_column()
    }
}
