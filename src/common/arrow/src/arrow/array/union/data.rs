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
use crate::arrow::array::UnionArray;
use crate::arrow::buffer::Buffer;
use crate::arrow::datatypes::DataType;

impl Arrow2Arrow for UnionArray {
    fn to_data(&self) -> ArrayData {
        let data_type = arrow_schema::DataType::from(self.data_type.clone());
        let len = self.len();

        let builder = match self.offsets.clone() {
            Some(offsets) => ArrayDataBuilder::new(data_type)
                .len(len)
                .buffers(vec![self.types.clone().into(), offsets.into()])
                .child_data(self.fields.iter().map(|x| to_data(x.as_ref())).collect()),
            None => ArrayDataBuilder::new(data_type)
                .len(len)
                .buffers(vec![self.types.clone().into()])
                .child_data(
                    self.fields
                        .iter()
                        .map(|x| to_data(x.as_ref()).slice(self.offset, len))
                        .collect(),
                ),
        };

        // Safety: Array is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let data_type: DataType = data.data_type().clone().into();

        let fields = data.child_data().iter().map(from_data).collect();
        let buffers = data.buffers();
        let mut types: Buffer<i8> = buffers[0].clone().into();
        types.slice(data.offset(), data.len());
        let offsets = match buffers.len() == 2 {
            true => {
                let mut offsets: Buffer<i32> = buffers[1].clone().into();
                offsets.slice(data.offset(), data.len());
                Some(offsets)
            }
            false => None,
        };

        // Map from type id to array index
        let map = match &data_type {
            DataType::Union(_, Some(ids), _) => {
                let mut map = [0; 127];
                for (pos, &id) in ids.iter().enumerate() {
                    map[id as usize] = pos;
                }
                Some(map)
            }
            DataType::Union(_, None, _) => None,
            _ => unreachable!("must be Union type"),
        };

        Self {
            types,
            map,
            fields,
            offsets,
            data_type,
            offset: data.offset(),
        }
    }
}
