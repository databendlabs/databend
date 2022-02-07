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
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::datatypes::DataType as ArrowType;

use crate::prelude::*;

mod mutable;

pub use mutable::*;
#[derive(Clone, Default)]
pub struct NullColumn {
    length: usize,
}

impl From<NullArray> for NullColumn {
    fn from(array: NullArray) -> Self {
        Self::new(array.len())
    }
}

impl NullColumn {
    pub fn new(length: usize) -> Self {
        Self { length }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self {
            length: array.len(),
        }
    }
}

impl Column for NullColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypePtr {
        Arc::new(NullType {})
    }

    fn is_null(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        self.length
    }

    fn null_at(&self, _row: usize) -> bool {
        true
    }

    fn only_null(&self) -> bool {
        true
    }

    fn validity(&self) -> (bool, Option<&Bitmap>) {
        (true, None)
    }

    fn memory_size(&self) -> usize {
        std::mem::size_of::<usize>()
    }

    fn as_arrow_array(&self) -> ArrayRef {
        Arc::new(NullArray::new_null(ArrowType::Null, self.length))
    }

    fn arc(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn slice(&self, _offset: usize, length: usize) -> ColumnRef {
        Arc::new(Self { length })
    }

    fn filter(&self, filter: &BooleanColumn) -> ColumnRef {
        let length = filter.values().len() - filter.values().null_count();
        Arc::new(Self { length })
    }

    fn scatter(&self, indices: &[usize], scattered_size: usize) -> Vec<ColumnRef> {
        let mut cnt = vec![0usize; scattered_size];
        for i in indices {
            cnt[*i] += 1;
        }

        cnt.iter()
            .map(|c| Arc::new(Self::new(*c)) as ColumnRef)
            .collect()
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        debug_assert!(
            offsets.len() == self.len(),
            "Size of offsets must match size of column"
        );

        Arc::new(Self {
            length: *offsets.last().unwrap() as usize,
        })
    }

    fn convert_full_column(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn get(&self, _index: usize) -> DataValue {
        DataValue::Null
    }
}

impl std::fmt::Debug for NullColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = if self.len() > 0 {
            vec!["NULL...".to_string()]
        } else {
            vec![]
        };
        let iter = data.iter();
        let head = "NullColumn";
        display_fmt(iter, head, self.len(), self.data_type_id(), f)
    }
}
