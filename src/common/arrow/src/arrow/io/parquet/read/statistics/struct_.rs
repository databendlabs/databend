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
use crate::arrow::array::MutableArray;
use crate::arrow::array::StructArray;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Result;

#[derive(Debug)]
pub struct DynMutableStructArray {
    data_type: DataType,
    pub inner: Vec<Box<dyn MutableArray>>,
}

impl DynMutableStructArray {
    pub fn try_with_capacity(data_type: DataType, capacity: usize) -> Result<Self> {
        let inners = match data_type.to_logical_type() {
            DataType::Struct(inner) => inner,
            _ => unreachable!(),
        };
        let inner = inners
            .iter()
            .map(|f| make_mutable(f.data_type(), capacity))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { data_type, inner })
    }
}
impl MutableArray for DynMutableStructArray {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn len(&self) -> usize {
        self.inner[0].len()
    }

    fn validity(&self) -> Option<&crate::arrow::bitmap::MutableBitmap> {
        None
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        let inner = self.inner.iter_mut().map(|x| x.as_box()).collect();

        Box::new(StructArray::new(self.data_type.clone(), inner, None))
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
