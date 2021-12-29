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

use common_arrow::arrow::array::MutableArray;
use common_arrow::arrow::array::MutableBinaryArray;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::MutableBuffer;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;

use crate::arrays::mutable::MutableArrayBuilder;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::DataType;

#[derive(Default)]
pub struct MutableStringArrayBuilder {
    builder: MutableBinaryArray<i64>,
}

impl MutableArrayBuilder for MutableStringArrayBuilder {
    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_series(&mut self) -> Series {
        self.builder.as_arc().into_series()
    }

    fn push_null(&mut self) {
        self.builder.push_null();
    }
}

impl MutableStringArrayBuilder {
    pub fn from_data(
        data_type: ArrowDataType,
        offsets: MutableBuffer<i64>,
        values: MutableBuffer<u8>,
        validity: Option<MutableBitmap>,
    ) -> Self {
        let builder = MutableBinaryArray::<i64>::from_data(data_type, offsets, values, validity);
        Self { builder }
    }

    pub fn push(&mut self, value: impl AsRef<[u8]>) {
        self.builder.push(Some(value));
    }

    pub fn push_option(&mut self, value: Option<impl AsRef<[u8]>>) {
        self.builder.push(value);
    }

    pub fn push_null(&mut self) {
        self.builder.push_null();
    }
}
