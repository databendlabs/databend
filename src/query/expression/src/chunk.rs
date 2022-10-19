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

use common_arrow::arrow::chunk::Chunk as ArrowChunk;
use common_arrow::ArrayRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::types::AnyType;
use crate::types::DataType;
use crate::ColumnBuilder;
use crate::Domain;
use crate::TypeSerializerImpl;
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
    pub fn column(&self, index: usize) -> &(Value<AnyType>, DataType) {
        &self.columns[index]
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

    #[inline]
    pub fn add_column(&mut self, column: Value<AnyType>, data_type: DataType) {
        self.columns.push((column, data_type))
    }

    pub fn get_serializers(&self) -> Result<Vec<TypeSerializerImpl>, String> {
        let mut serializers = Vec::with_capacity(self.num_columns());
        for (column, data_type) in self.columns() {
            let serializer = data_type.create_serializer(column)?;
            serializers.push(serializer);
        }
        Ok(serializers)
    }
}

impl TryFrom<Chunk> for ArrowChunk<ArrayRef> {
    type Error = ErrorCode;

    fn try_from(v: Chunk) -> Result<ArrowChunk<ArrayRef>> {
        let arrays = v
            .columns()
            .iter()
            .map(|(val, _)| {
                let column = val.clone().into_column().unwrap();
                column.as_arrow()
            })
            .collect();

        Ok(ArrowChunk::try_new(arrays)?)
    }
}
