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
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::ScalarColumn;
use common_datavalues::StringColumn;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeID;
use common_exception::Result;
use common_io::prelude::FormatSettings;

use crate::pipelines::processors::AggregatorParams;

pub trait GroupColumnsBuilder<KeyRef> {
    fn append_value(&mut self, v: KeyRef);
    fn finish(self) -> Result<Vec<ColumnRef>>;
}

pub struct FixedKeysGroupColumnsBuilder<T> {
    data: Vec<T>,
    groups_fields: Vec<DataField>,
}

impl<T> FixedKeysGroupColumnsBuilder<T>
where for<'a> HashMethodFixedKeys<T>: HashMethod<HashKey = T>
{
    pub fn create(capacity: usize, params: &AggregatorParams) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            groups_fields: params.group_data_fields.clone(),
        }
    }
}

impl<T: Copy + Send + Sync + 'static> GroupColumnsBuilder<T> for FixedKeysGroupColumnsBuilder<T>
where for<'a> HashMethodFixedKeys<T>: HashMethod<HashKey = T>
{
    #[inline]
    fn append_value(&mut self, v: T) {
        self.data.push(v);
    }

    #[inline]
    fn finish(self) -> Result<Vec<ColumnRef>> {
        let method = HashMethodFixedKeys::<T>::default();
        method.deserialize_group_columns(self.data, &self.groups_fields)
    }
}

pub struct SerializedKeysGroupColumnsBuilder {
    data: Vec<*const [u8]>,
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

impl GroupColumnsBuilder<*const [u8]> for SerializedKeysGroupColumnsBuilder {
    fn append_value(&mut self, v: *const [u8]) {
        self.data.push(v);
    }

    fn finish(self) -> Result<Vec<ColumnRef>> {
        let mut keys = Vec::with_capacity(self.data.len());

        for v in &self.data {
            unsafe {
                keys.push(v.as_ref().unwrap());
            }
        }

        if self.groups_fields.len() == 1
            && self.groups_fields[0].data_type().data_type_id() == TypeID::String
        {
            let col = StringColumn::from_slice(&keys);
            return Ok(vec![col.arc()]);
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
