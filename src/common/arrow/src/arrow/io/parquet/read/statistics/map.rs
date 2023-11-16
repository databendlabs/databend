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

use super::make_mutable;
use crate::arrow::array::Array;
use crate::arrow::array::MapArray;
use crate::arrow::array::MutableArray;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Error;

#[derive(Debug)]
pub struct DynMutableMapArray {
    data_type: DataType,
    pub inner: Box<dyn MutableArray>,
}

impl DynMutableMapArray {
    pub fn try_with_capacity(data_type: DataType, capacity: usize) -> Result<Self, Error> {
        let inner = match data_type.to_logical_type() {
            DataType::Map(inner, _) => inner,
            _ => unreachable!(),
        };
        let inner = make_mutable(inner.data_type(), capacity)?;

        Ok(Self { data_type, inner })
    }
}

impl MutableArray for DynMutableMapArray {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn validity(&self) -> Option<&crate::arrow::bitmap::MutableBitmap> {
        None
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(MapArray::new(
            self.data_type.clone(),
            vec![0, self.inner.len() as i32].try_into().unwrap(),
            self.inner.as_box(),
            None,
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn push_null(&mut self) {
        todo!()
    }

    fn reserve(&mut self, _: usize) {
        todo!();
    }

    fn shrink_to_fit(&mut self) {
        todo!()
    }
}
