// Copyright 2020 Datafuse Labs.
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

use crate::prelude::*;

pub struct BinaryArrayBuilder {
    builder: MutableBinaryArray<i64>,
}

impl BinaryArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: MutableBinaryArray::<i64>::with_capacity(capacity),
        }
    }

    pub fn append_value(&mut self, value: impl AsRef<[u8]>) {
        self.builder.push(Some(value))
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.builder.push_null();
    }

    pub fn finish(&mut self) -> DFBinaryArray {
        let array = self.builder.as_arc();
        DFBinaryArray::from_arrow_array(array.as_ref())
    }
}
