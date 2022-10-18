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
use common_datavalues::prelude::*;

/// Remove the group by key from the state and rebuild it into a column
pub trait KeysColumnBuilder {
    type T;

    fn append_value(&mut self, v: Self::T);
    fn finish(self) -> ColumnRef;
}

pub struct FixedKeysColumnBuilder<T>
where T: PrimitiveType
{
    pub inner_builder: MutablePrimitiveColumn<T>,
}

impl<T> KeysColumnBuilder for FixedKeysColumnBuilder<T>
where
    T: PrimitiveType,
    for<'a> HashMethodFixedKeys<T>: HashMethod<HashKey = T>,
{
    type T = T;

    #[inline]
    fn finish(mut self) -> ColumnRef {
        self.inner_builder.to_column()
    }

    #[inline]
    fn append_value(&mut self, v: T) {
        self.inner_builder.append_value(v)
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

    fn finish(mut self) -> ColumnRef {
        self.inner_builder.to_column()
    }

    fn append_value(&mut self, v: &'a [u8]) {
        self.inner_builder.append_value(v);
    }
}

pub struct LargeFixedKeysColumnBuilder<T>
where T: LargePrimitive
{
    pub inner_builder: MutableStringColumn,
    pub _t: PhantomData<T>,
}

impl<T> KeysColumnBuilder for LargeFixedKeysColumnBuilder<T>
where
    T: LargePrimitive,
    for<'a> HashMethodFixedKeys<T>: HashMethod<HashKey = T>,
{
    type T = T;

    #[inline]
    fn finish(mut self) -> ColumnRef {
        self.inner_builder.to_column()
    }

    #[inline]
    fn append_value(&mut self, v: T) {
        let values = self.inner_builder.values_mut();
        let new_len = values.len() + T::BYTE_SIZE;
        values.resize(new_len, 0);
        v.serialize_to(&mut values[new_len - T::BYTE_SIZE..]);
        self.inner_builder.offsets_mut().push(new_len as i64);
    }
}
