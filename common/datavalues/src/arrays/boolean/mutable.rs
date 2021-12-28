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

use common_arrow::arrow::array::MutableBooleanArray;
use common_arrow::arrow::array::MutableArray;
use common_arrow::arrow::array::Array;

use crate::arrays::mutable::MutableArrayBuilder;
use super::DFBooleanArray;
use crate::DataType;

pub struct MutableBooleanArrayBuilder {
    builder: MutableBooleanArray,
}

// TODO(veeupup) make arrow2 array builder originally use here
impl MutableArrayBuilder for MutableBooleanArrayBuilder {
    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_arc(&mut self) -> std::sync::Arc<dyn Array> {
        self.builder.as_arc()
    }
}

impl MutableBooleanArrayBuilder {
    pub fn new() -> Self {
        MutableBooleanArrayBuilder { builder: MutableBooleanArray::new() }
    }

    pub fn push(&mut self, value: bool) {
        self.builder.push(Some(value));
    }

    pub fn push_option(&mut self, value: Option<bool>) {
        self.builder.push(value);
    }

    pub fn push_null(&mut self) {
        self.builder.push_null();
    }

    fn build(&mut self) -> DFBooleanArray {
        let array = self.builder.as_arc();
        DFBooleanArray::from_arrow_array(array.as_ref())
    }
}


