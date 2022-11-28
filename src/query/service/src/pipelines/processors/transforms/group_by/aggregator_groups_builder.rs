// Copyright 2022 Datafuse Labs.
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

use common_datablocks::HashMethodFixedKeys;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_datavalues::ScalarColumn;
use common_datavalues::StringColumn;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeID;
use common_exception::Result;

use crate::pipelines::processors::AggregatorParams;

pub trait GroupColumnsBuilder {
    type T;
    fn append_value(&mut self, v: Self::T);
    fn finish(self) -> Result<Vec<ColumnRef>>;
}

pub struct FixedKeysGroupColumnsBuilder<'a, T> {
    _t: PhantomData<&'a ()>,
    data: Vec<T>,
    group_column_indices: Vec<usize>,
    group_data_types: Vec<DataTypeImpl>,
}

impl<'a, T> FixedKeysGroupColumnsBuilder<'a, T> {
    pub fn create(capacity: usize, params: &AggregatorParams) -> Self {
        Self {
            _t: Default::default(),
            data: Vec::with_capacity(capacity),
            group_column_indices: params.group_columns.clone(),
            group_data_types: params.group_data_types.clone(),
        }
    }
}

impl<'a, T: Copy + Send + Sync + 'static> GroupColumnsBuilder
    for FixedKeysGroupColumnsBuilder<'a, T>
{
    type T = &'a T;

    #[inline]
    fn append_value(&mut self, v: Self::T) {
        self.data.push(*v);
    }

    #[inline]
    fn finish(self) -> Result<Vec<ColumnRef>> {
        let method = HashMethodFixedKeys::<T>::default();
        method.deserialize_group_columns(
            self.data,
            &self
                .group_column_indices
                .iter()
                .cloned()
                .zip(self.group_data_types.iter().cloned())
                .collect::<Vec<_>>(),
        )
    }
}

pub struct SerializedKeysGroupColumnsBuilder<'a> {
    data: Vec<&'a [u8]>,
    group_data_types: Vec<DataTypeImpl>,
}

impl<'a> SerializedKeysGroupColumnsBuilder<'a> {
    pub fn create(capacity: usize, params: &AggregatorParams) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            group_data_types: params.group_data_types.clone(),
        }
    }
}

impl<'a> GroupColumnsBuilder for SerializedKeysGroupColumnsBuilder<'a> {
    type T = &'a [u8];

    fn append_value(&mut self, v: &'a [u8]) {
        self.data.push(v);
    }

    fn finish(mut self) -> Result<Vec<ColumnRef>> {
        let rows = self.data.len();
        let keys = self.data.as_mut_slice();

        if self.group_data_types.len() == 1
            && self.group_data_types[0].data_type_id() == TypeID::String
        {
            let col = StringColumn::from_slice(keys);
            return Ok(vec![col.arc()]);
        }

        let mut res = Vec::with_capacity(self.group_data_types.len());
        for data_type in self.group_data_types.iter() {
            let mut deserializer = data_type.create_deserializer(rows);

            for (_, key) in keys.iter_mut().enumerate() {
                deserializer.de_binary(key)?;
            }
            res.push(deserializer.finish_to_column());
        }

        Ok(res)
    }
}
