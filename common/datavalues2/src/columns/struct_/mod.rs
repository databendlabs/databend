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

use std::sync::Arc;

use common_arrow::arrow::array::*;

use crate::prelude::*;

#[derive(Clone)]
pub struct StructColumn {
    values: Vec<ColumnRef>,
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
        let values: Vec<ColumnRef> = array
            .values()
            .iter()
            .map(|v| v.clone().into_column())
            .collect();

        debug_assert!(!values.is_empty());
        Self { values, data_type }
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

    pub fn from_data(values: Vec<ColumnRef>, data_type: DataTypePtr) -> Self {
        Self { values, data_type }
    }

    pub fn values(&self) -> &[ColumnRef] {
        &self.values
    }
}

impl Column for StructColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypePtr {
        self.data_type.clone()
    }

    fn len(&self) -> usize {
        self.values[0].len()
    }

    fn memory_size(&self) -> usize {
        self.values.iter().map(|v| v.memory_size()).sum()
    }

    fn as_arrow_array(&self) -> ArrayRef {
        let arrow_type = self.data_type().arrow_type();
        let arrays = self.values.iter().map(|v| v.as_arrow_array()).collect();
        Arc::new(StructArray::from_data(arrow_type, arrays, None))
    }

    fn arc(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        let values = self
            .values
            .iter()
            .map(|v| v.slice(offset, length))
            .collect();

        Arc::new(Self {
            values,
            data_type: self.data_type.clone(),
        })
    }

    fn filter(&self, filter: &BooleanColumn) -> ColumnRef {
        let values = self.values.iter().map(|v| v.filter(filter)).collect();

        Arc::new(Self {
            values,
            data_type: self.data_type.clone(),
        })
    }

    fn scatter(&self, indices: &[usize], scattered_size: usize) -> Vec<ColumnRef> {
        let values: Vec<Vec<ColumnRef>> = self
            .values
            .iter()
            .map(|v| v.scatter(indices, scattered_size))
            .collect();

        let mut result = Vec::with_capacity(scattered_size);

        for s in 0..scattered_size {
            let mut arrays = Vec::with_capacity(self.values.len());
            for value in values.iter() {
                arrays.push(value[s].clone());
            }
            result.push(
                Arc::new(StructColumn::from_data(arrays, self.data_type.clone())) as ColumnRef,
            );
        }
        result
    }

    fn get(&self, index: usize) -> DataValue {
        let values = self.values.iter().map(|v| v.get(index)).collect();
        DataValue::Struct(values)
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        let values = self.values.iter().map(|v| v.replicate(offsets)).collect();

        Arc::new(Self {
            values,
            data_type: self.data_type.clone(),
        })
    }

    fn convert_full_column(&self) -> ColumnRef {
        Arc::new(self.clone())
    }
}

impl std::fmt::Debug for StructColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut data = Vec::new();
        for idx in 0..self.len() {
            let x = self.get(idx);
            data.push(format!("{:?}", x));
        }
        let head = "StructColumn";
        let iter = data.iter();
        display_fmt(iter, head, self.len(), self.data_type_id(), f)
    }
}
