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
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::types::Index;

use crate::prelude::*;

mod iterator;
mod mutable;

pub use iterator::*;
pub use mutable::*;

type LargeListArray = ListArray<i64>;

#[derive(Clone)]
pub struct ArrayColumn {
    data_type: DataTypeImpl,
    offsets: Buffer<i64>,
    values: ColumnRef,
}

impl ArrayColumn {
    pub fn new(array: LargeListArray) -> Self {
        let ty = array.data_type();

        let data_type = if let ArrowType::LargeList(f) = ty {
            let ty = from_arrow_field(f);
            DataTypeImpl::Array(ArrayType::create(ty))
        } else {
            unreachable!()
        };

        Self {
            data_type,
            offsets: array.offsets().clone(),
            values: array.values().clone().into_column(),
        }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .clone(),
        )
    }

    pub fn from_data(data_type: DataTypeImpl, offsets: Buffer<i64>, values: ColumnRef) -> Self {
        Self {
            data_type,
            offsets,
            values,
        }
    }

    #[inline]
    pub fn size_at_index(&self, i: usize) -> usize {
        let offset = self.offsets[i];
        let offset_1 = self.offsets[i + 1];
        (offset_1 - offset).to_usize()
    }

    pub fn values(&self) -> &ColumnRef {
        &self.values
    }

    pub fn offsets(&self) -> &[i64] {
        self.offsets.as_slice()
    }
}

impl Column for ArrayColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypeImpl {
        self.data_type.clone()
    }

    fn column_type_name(&self) -> String {
        "Array".to_string()
    }

    fn column_meta(&self) -> ColumnMeta {
        let data_type: ArrayType = self.data_type.clone().try_into().unwrap();
        ColumnMeta::Array {
            inner_type: data_type.inner_type().clone(),
        }
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn memory_size(&self) -> usize {
        self.values.memory_size() + self.offsets.len() * std::mem::size_of::<i64>()
    }

    fn as_arrow_array(&self) -> ArrayRef {
        let arrow_type = self.data_type().arrow_type();
        let array = self.values.as_arrow_array();
        Arc::new(LargeListArray::from_data(
            arrow_type,
            self.offsets.clone(),
            array,
            None,
        ))
    }

    fn arc(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        unsafe {
            let offsets = self.offsets.clone().slice_unchecked(offset, length + 1);
            Arc::new(Self {
                data_type: self.data_type.clone(),
                offsets,
                values: self.values.clone(),
            })
        }
    }

    fn scatter(&self, indices: &[usize], scattered_size: usize) -> Vec<ColumnRef> {
        scatter_scalar_column(self, indices, scattered_size)
    }

    fn filter(&self, filter: &BooleanColumn) -> ColumnRef {
        filter_scalar_column(self, filter)
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        replicate_scalar_column(self, offsets)
    }

    fn convert_full_column(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn get(&self, index: usize) -> DataValue {
        let offset = self.offsets[index] as usize;
        let length = self.size_at_index(index);
        let values = (offset..offset + length)
            .map(|i| self.values.get(i))
            .collect();
        DataValue::Array(values)
    }
}

impl ScalarColumn for ArrayColumn {
    type Builder = MutableArrayColumn;
    type OwnedItem = ArrayValue;
    type RefItem<'a> = <ArrayValue as Scalar>::RefType<'a>;
    type Iterator<'a> = ArrayValueIter<'a>;

    #[inline]
    fn get_data(&self, idx: usize) -> Self::RefItem<'_> {
        ArrayValueRef::Indexed { column: self, idx }
    }

    fn scalar_iter(&self) -> Self::Iterator<'_> {
        ArrayValueIter::new(self)
    }
}

impl std::fmt::Debug for ArrayColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut data = Vec::with_capacity(self.len());
        for idx in 0..self.len() {
            let x = self.get(idx);
            data.push(format!("{:?}", x));
        }
        let head = "ArrayColumn";
        let iter = data.iter();
        display_fmt(iter, head, self.len(), self.data_type_id(), f)
    }
}
