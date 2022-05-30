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

use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datavalues::ColumnRef;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::MutableColumn;
use common_datavalues::MutableStringColumn;
use common_datavalues::ScalarColumnBuilder;
use common_datavalues::TypeDeserializer;
use common_exception::Result;
use common_io::prelude::FormatSettings;

use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::transforms::group_by::keys_ref::KeysRef;

pub trait GroupColumnsBuilder<Key> {
    fn append_value(&mut self, v: &Key);
    fn finish(self) -> Result<Vec<ColumnRef>>;
}

pub struct FixedKeysGroupColumnsBuilder<T>
{
    data: Vec<T>,
    groups_fields: Vec<DataField>,
}

impl<T> FixedKeysGroupColumnsBuilder<T>
where
    for<'a> HashMethodFixedKeys<T>: HashMethod<HashKey<'a> = T>,
{
    pub fn create(capacity: usize, params: &AggregatorParams) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            groups_fields: params.group_data_fields.clone(),
        }
    }
}

impl<T: Copy + Send + Sync + 'static> GroupColumnsBuilder<T> for FixedKeysGroupColumnsBuilder<T>
where
    for<'a> HashMethodFixedKeys<T>: HashMethod<HashKey<'a> = T>,
{
    #[inline]
    fn append_value(&mut self, v: &T) {
        self.data.push(*v);
    }

    #[inline]
    fn finish(self) -> Result<Vec<ColumnRef>> {
        let method = HashMethodFixedKeys::<T>::default();
        method.deserialize_group_columns(self.data, &self.groups_fields)
    }
}


pub struct SerializedKeysGroupColumnsBuilder {
    data: Vec<KeysRef>,
    groups_fields: Vec<DataField>,
}

impl SerializedKeysGroupColumnsBuilder {
    pub fn create(capacity: usize, params: &AggregatorParams) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            groups_fields: params.group_data_fields.clone(),
        }
    }
}

impl GroupColumnsBuilder<KeysRef> for SerializedKeysGroupColumnsBuilder {
    fn append_value(&mut self, v: &KeysRef) {
        self.data.push(*v);
    }

    fn finish(self) -> Result<Vec<ColumnRef>> {
        let mut keys = Vec::with_capacity(self.data.len());

        for v in &self.data {
            unsafe {
                let value = std::slice::from_raw_parts(v.address as *const u8, v.length);
                keys.push(value);
            }
        }

        let rows = self.data.len();
        let mut res = Vec::with_capacity(self.groups_fields.len());
        let format = FormatSettings::default();
        for group_field in self.groups_fields.iter() {
            let data_type = group_field.data_type();
            let mut deserializer = data_type.create_deserializer(rows);

            for (_, key) in keys.iter_mut().enumerate() {
                deserializer.de_binary(key, &format)?;
            }
            res.push(deserializer.finish_to_column());
        }

        Ok(res)
    }
}

pub struct SingleStringGroupColumnsBuilder {
    inner_builder: MutableStringColumn,
}

impl SingleStringGroupColumnsBuilder {
    pub fn create(capacity: usize, params: &AggregatorParams) -> Self {
        assert_eq!(params.group_data_fields.len(), 1);

        Self {
            inner_builder: MutableStringColumn::with_capacity(capacity),
        }
    }
}

impl GroupColumnsBuilder<KeysRef> for SingleStringGroupColumnsBuilder {
    fn append_value(&mut self, v: &KeysRef) {
        unsafe {
            let value = std::slice::from_raw_parts(v.address as *const u8, v.length);
            self.inner_builder.push(value);
        }
    }

    fn finish(self) -> Result<Vec<ColumnRef>> {
        let mut builder = self.inner_builder;
        let col = builder.to_column();
        Ok(vec![col])
    }
}
