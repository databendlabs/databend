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
use crate::arrow::array::*;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Result;
use crate::arrow::offset::Offsets;

#[derive(Debug)]
pub struct DynMutableListArray {
    data_type: DataType,
    pub inner: Box<dyn MutableArray>,
}

impl DynMutableListArray {
    pub fn try_with_capacity(data_type: DataType, capacity: usize) -> Result<Self> {
        let inner = match data_type.to_logical_type() {
            DataType::List(inner) | DataType::LargeList(inner) => inner.data_type(),
            _ => unreachable!(),
        };
        let inner = make_mutable(inner, capacity)?;

        Ok(Self { data_type, inner })
    }
}

impl MutableArray for DynMutableListArray {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn validity(&self) -> Option<&crate::arrow::bitmap::MutableBitmap> {
        self.inner.validity()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        let inner = self.inner.as_box();

        match self.data_type.to_logical_type() {
            DataType::List(_) => {
                let offsets =
                    Offsets::try_from_lengths(std::iter::repeat(1).take(inner.len())).unwrap();
                Box::new(ListArray::<i32>::new(
                    self.data_type.clone(),
                    offsets.into(),
                    inner,
                    None,
                ))
            }
            DataType::LargeList(_) => {
                let offsets =
                    Offsets::try_from_lengths(std::iter::repeat(1).take(inner.len())).unwrap();
                Box::new(ListArray::<i64>::new(
                    self.data_type.clone(),
                    offsets.into(),
                    inner,
                    None,
                ))
            }
            _ => unreachable!(),
        }
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
