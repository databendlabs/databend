// Copyright 2020-2022 Jorge C. Leitão
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

use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;

use crate::arrow::array::from_data;
use crate::arrow::array::to_data;
use crate::arrow::array::Arrow2Arrow;
use crate::arrow::array::DictionaryArray;
use crate::arrow::array::DictionaryKey;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::PhysicalType;

impl<K: DictionaryKey> Arrow2Arrow for DictionaryArray<K> {
    fn to_data(&self) -> ArrayData {
        let keys = self.keys.to_data();
        let builder = keys
            .into_builder()
            .data_type(self.data_type.clone().into())
            .child_data(vec![to_data(self.values.as_ref())]);

        // Safety: Dictionary is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let key = match data.data_type() {
            arrow_schema::DataType::Dictionary(k, _) => k.as_ref(),
            d => panic!("unsupported dictionary type {d}"),
        };

        let data_type = DataType::from(data.data_type().clone());
        assert_eq!(
            data_type.to_physical_type(),
            PhysicalType::Dictionary(K::KEY_TYPE)
        );

        let key_builder = ArrayDataBuilder::new(key.clone())
            .buffers(vec![data.buffers()[0].clone()])
            .offset(data.offset())
            .len(data.len())
            .nulls(data.nulls().cloned());

        // Safety: Dictionary is valid
        let key_data = unsafe { key_builder.build_unchecked() };
        let keys = PrimitiveArray::from_data(&key_data);
        let values = from_data(&data.child_data()[0]);

        Self {
            data_type,
            keys,
            values,
        }
    }
}
