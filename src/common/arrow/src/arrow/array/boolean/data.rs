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

use arrow_buffer::BooleanBuffer;
use arrow_buffer::NullBuffer;
use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;

use crate::arrow::array::Arrow2Arrow;
use crate::arrow::array::BooleanArray;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::datatypes::DataType;

impl Arrow2Arrow for BooleanArray {
    fn to_data(&self) -> ArrayData {
        let buffer = NullBuffer::from(self.values.clone());

        let builder = ArrayDataBuilder::new(arrow_schema::DataType::Boolean)
            .len(buffer.len())
            .offset(buffer.offset())
            .buffers(vec![buffer.into_inner().into_inner()])
            .nulls(self.validity.as_ref().map(|b| b.clone().into()));

        // Safety: Array is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        assert_eq!(data.data_type(), &arrow_schema::DataType::Boolean);

        let buffers = data.buffers();
        let buffer = BooleanBuffer::new(buffers[0].clone(), data.offset(), data.len());
        // Use NullBuffer to compute set count
        let values = Bitmap::from_null_buffer(NullBuffer::new(buffer));

        Self {
            data_type: DataType::Boolean,
            values,
            validity: data.nulls().map(|n| Bitmap::from_null_buffer(n.clone())),
        }
    }
}
