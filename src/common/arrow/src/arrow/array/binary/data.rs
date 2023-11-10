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

use crate::arrow::array::Arrow2Arrow;
use crate::arrow::array::BinaryArray;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::offset::Offset;
use crate::arrow::offset::OffsetsBuffer;

impl<O: Offset> Arrow2Arrow for BinaryArray<O> {
    fn to_data(&self) -> ArrayData {
        let data_type = self.data_type.clone().into();
        let builder = ArrayDataBuilder::new(data_type)
            .len(self.offsets().len_proxy())
            .buffers(vec![
                self.offsets.clone().into_inner().into(),
                self.values.clone().into(),
            ])
            .nulls(self.validity.as_ref().map(|b| b.clone().into()));

        // Safety: Array is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let data_type = data.data_type().clone().into();

        if data.is_empty() {
            // Handle empty offsets
            return Self::new_empty(data_type);
        }

        let buffers = data.buffers();

        // Safety: ArrayData is valid
        let mut offsets = unsafe { OffsetsBuffer::new_unchecked(buffers[0].clone().into()) };
        offsets.slice(data.offset(), data.len() + 1);

        Self {
            data_type,
            offsets,
            values: buffers[1].clone().into(),
            validity: data.nulls().map(|n| Bitmap::from_null_buffer(n.clone())),
        }
    }
}
