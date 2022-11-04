// Copyright 2022 Datafuse Labs.
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

use std::ops::Range;

use crate::types::AnyType;
use crate::types::DataType;
use crate::ColumnBuilder;
use crate::Domain;
use crate::TypeSerializer;
use crate::Value;

/// Chunk is a lightweight container for a group of columns.
#[derive(Clone)]
pub struct Chunk {
    columns: Vec<(Value<AnyType>, DataType)>,
    num_rows: usize,
}

impl Chunk {
    #[inline]
    pub fn new(columns: Vec<(Value<AnyType>, DataType)>, num_rows: usize) -> Self {
        debug_assert!(columns.iter().all(|(col, _)| match col {
            Value::Scalar(_) => true,
            Value::Column(c) => c.len() == num_rows,
        }));
        Self { columns, num_rows }
    }

    #[inline]
    pub fn empty() -> Self {
        Chunk::new(vec![], 0)
    }

    #[inline]
    pub fn columns(&self) -> &[(Value<AnyType>, DataType)] {
        &self.columns
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_columns() == 0 || self.num_rows() == 0
    }

    #[inline]
    pub fn domains(&self) -> Vec<Domain> {
        self.columns
            .iter()
            .map(|(value, _)| value.as_ref().domain())
            .collect()
    }

    #[inline]
    pub fn memory_size(&self) -> usize {
        self.columns()
            .iter()
            .map(|(col, _)| match col {
                Value::Scalar(s) => std::mem::size_of_val(s) * self.num_rows,
                Value::Column(c) => c.memory_size(),
            })
            .sum()
    }

    pub fn convert_to_full(&self) -> Self {
        let columns = self
            .columns()
            .iter()
            .map(|(col, ty)| match col {
                Value::Scalar(s) => {
                    let builder = ColumnBuilder::repeat(&s.as_ref(), self.num_rows, ty);
                    let col = builder.build();
                    (Value::Column(col), ty.clone())
                }
                Value::Column(c) => (Value::Column(c.clone()), ty.clone()),
            })
            .collect();
        Self {
            columns,
            num_rows: self.num_rows,
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let columns = self
            .columns()
            .iter()
            .map(|(col, ty)| match col {
                Value::Scalar(s) => (Value::Scalar(s.clone()), ty.clone()),
                Value::Column(c) => (Value::Column(c.slice(range.clone())), ty.clone()),
            })
            .collect();
        Self {
            columns,
            num_rows: range.end - range.start + 1,
        }
    }

    pub fn get_serializers(&self) -> Result<Vec<Box<dyn TypeSerializer>>, String> {
        let mut serializers = Vec::with_capacity(self.num_columns());
        for (value, data_type) in self.columns() {
            let column = match value {
                Value::Scalar(s) => ColumnBuilder::repeat(&s.as_ref(), 1, data_type).build(),
                Value::Column(c) => c.clone(),
            };
            let serializer = data_type.create_serializer(column)?;
            serializers.push(serializer);
        }
        Ok(serializers)
    }
}
