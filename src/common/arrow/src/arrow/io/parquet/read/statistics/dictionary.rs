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
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::error::Result;

#[derive(Debug)]
pub struct DynMutableDictionary {
    data_type: DataType,
    pub inner: Box<dyn MutableArray>,
}

impl DynMutableDictionary {
    pub fn try_with_capacity(data_type: DataType, capacity: usize) -> Result<Self> {
        let inner = if let DataType::Dictionary(_, inner, _) = &data_type {
            inner.as_ref()
        } else {
            unreachable!()
        };
        let inner = make_mutable(inner, capacity)?;

        Ok(Self { data_type, inner })
    }
}

impl MutableArray for DynMutableDictionary {
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
        match self.data_type.to_physical_type() {
            PhysicalType::Dictionary(key) => match_integer_type!(key, |$T| {
                let keys: Vec<$T> = (0..inner.len() as $T).collect();
                let keys = PrimitiveArray::<$T>::from_vec(keys);
                Box::new(DictionaryArray::<$T>::try_new(self.data_type.clone(), keys, inner).unwrap())
            }),
            _ => todo!(),
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
