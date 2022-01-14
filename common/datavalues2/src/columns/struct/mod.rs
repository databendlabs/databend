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

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::Bitmap;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct StructColumn {
    values: Vec<ArrayRef>,
    data_type: DataTypePtr,
}

impl From<StructArray> for StructColumn {
    fn from(array: StructArray) -> Self {
        Self::new(array)
    }
}

impl StructColumn {
    pub fn new(array: StructArray) -> Self {
        let data_type = from_arrow_type(array.data_type());
        Self {
            values: array.values().to_vec(),
            data_type,
        }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(
            array
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .clone(),
        )
    }

    pub fn data_type(&self) -> DataTypePtr {
        self.data_type.clone()
    }
}

impl Column for StructColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn data_type(&self) -> DataTypePtr {
        todo!()
    }

    fn is_nullable(&self) -> bool {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn null_at(&self, row: usize) -> bool {
        todo!()
    }

    fn validity(&self) -> (bool, Option<&Bitmap>) {
        todo!()
    }

    fn memory_size(&self) -> usize {
        todo!()
    }

    fn as_arrow_array(&self) -> ArrayRef {
        todo!()
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        todo!()
    }

    unsafe fn get_unchecked(&self, index: usize) -> DataValue {
        todo!()
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        todo!()
    }
}
