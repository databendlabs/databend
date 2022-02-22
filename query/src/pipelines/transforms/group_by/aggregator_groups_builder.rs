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
use common_datavalues::PrimitiveType;
use common_exception::Result;

use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::transforms::group_by::keys_ref::KeysRef;

pub trait GroupColumnsBuilder<Key> {
    fn append_value(&mut self, v: &Key);
    fn finish(self) -> Result<Vec<ColumnRef>>;
}

pub struct FixedKeysGroupColumnsBuilder<T>
where T: PrimitiveType
{
    data: Vec<T>,
    groups_fields: Vec<DataField>,
}

impl<T> FixedKeysGroupColumnsBuilder<T>
where
    T: PrimitiveType,
    HashMethodFixedKeys<T>: HashMethod<HashKey = T>,
{
    pub fn create(capacity: usize, params: &AggregatorParams) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            groups_fields: params.group_data_fields.clone(),
        }
    }
}

impl<T> GroupColumnsBuilder<T> for FixedKeysGroupColumnsBuilder<T>
where
    T: PrimitiveType,
    HashMethodFixedKeys<T>: HashMethod<HashKey = T>,
{
    #[inline]
    fn append_value(&mut self, v: &T) {
        self.data.push(*v);
    }

    #[inline]
    fn finish(self) -> Result<Vec<ColumnRef>> {
        let mut keys = self.data;
        let rows = keys.len();
        let step = std::mem::size_of::<T>();
        let length = rows * step;
        let capacity = keys.capacity() * step;
        let mutptr = keys.as_mut_ptr() as *mut u8;
        let vec8 = unsafe {
            std::mem::forget(keys);
            // construct new vec
            Vec::from_raw_parts(mutptr, length, capacity)
        };

        let mut offsize = 0;
        let mut groups_columns = Vec::with_capacity(self.groups_fields.len());

        for group_field in self.groups_fields.iter() {
            let data_type = group_field.data_type();
            let mut deserializer = data_type.create_deserializer(rows);
            let reader = vec8.as_slice();
            deserializer.de_batch(&reader[offsize..], step, rows)?;
            groups_columns.push(deserializer.finish_to_column());
            offsize += data_type.data_type_id().numeric_byte_size()?;
        }

        Ok(groups_columns)
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
        for group_field in self.groups_fields.iter() {
            let data_type = group_field.data_type();
            let mut deserializer = data_type.create_deserializer(rows);

            for (_row, key) in keys.iter_mut().enumerate() {
                deserializer.de(key)?;
            }
            res.push(deserializer.finish_to_column());
        }

        Ok(res)
    }
}
