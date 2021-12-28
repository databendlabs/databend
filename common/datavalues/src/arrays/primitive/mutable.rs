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

use common_arrow::arrow::array::MutablePrimitiveArray;
use common_arrow::arrow::array::MutableArray;
use common_arrow::arrow::array::Array;

use crate::arrays::mutable::MutableArrayBuilder;
use crate::prelude::*;

pub struct MutablePrimitiveArrayBuilder<T>
where T: DFPrimitiveType
{
    builder: MutablePrimitiveArray<T>,
}

impl<T> MutableArrayBuilder for MutablePrimitiveArrayBuilder<T>
where T: DFPrimitiveType {
    fn data_type(&self) -> DataType {
        let datatype: DataType = self.builder.data_type().into();
        datatype
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

// TODO(veeupup) make arrow2 array builder originally use here
impl<T> MutablePrimitiveArrayBuilder<T> 
where T: DFPrimitiveType
{
    pub fn new() -> Self {
        MutablePrimitiveArrayBuilder {
            builder: MutablePrimitiveArray::<T>::new()
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        MutablePrimitiveArrayBuilder {
            builder: MutablePrimitiveArray::<T>::with_capacity(capacity)
        }
    }

    pub fn push(&mut self, v: T) {
        self.builder.push(Some(v));
    }

    pub fn push_option(&mut self, v: Option<T>) {
        self.builder.push(v)
    }

    pub fn push_null(&mut self) {
        self.builder.push_null();
    }

    fn build(&mut self) -> DFPrimitiveArray<T> {
        let array = self.builder.as_arc();
        DFPrimitiveArray::<T>::from_arrow_array(array.as_ref())
    }
}
