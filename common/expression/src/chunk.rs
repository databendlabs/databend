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
use crate::Value;

/// Chunk is a lightweight container for a group of columns.
#[derive(Clone)]
pub struct Chunk {
    columns: Vec<Value<AnyType>>,
    num_rows: usize,
}

impl Chunk {
    #[inline]
    pub fn new(columns: Vec<Value<AnyType>>, num_rows: usize) -> Self {
        debug_assert!(
            columns
                .iter()
                .filter(|value| match value {
                    Value::Scalar(_) => false,
                    Value::Column(c) => c.len() != num_rows,
                })
                .count()
                == 0
        );
        Self { columns, num_rows }
    }

    #[inline]
    pub fn empty() -> Self {
        Chunk::new(vec![], 0)
    }

    #[inline]
    pub fn columns(&self) -> &[Value<AnyType>] {
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
            .map(|value| value.as_ref().domain())
            .collect()
    }
    
    #[inline]
    pub fn memory_size(&self) -> usize {
        self.columns()
            .iter()
            .map(|c| match c {
                Value::Scalar(s) => std::mem::size_of_val(s) * self.num_rows,
                Value::Column(c) => c.memory_size(),
            })
            .sum()
    }

    pub fn convert_to_full(&self) -> Self {
        let mut columns = Vec::with_capacity(self.num_columns());
        for col in self.columns() {
            match col {
                Value::Scalar(s) => {
                    let builder = s.as_ref().repeat(self.num_rows);
                    let col = builder.build();
                    columns.push(Value::Column(col));
                }
                Value::Column(c) => columns.push(Value::Column(c.clone())),
            }
        }
        Self {
            columns,
            num_rows: self.num_rows,
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let mut columns = Vec::with_capacity(self.num_columns());
        for col in self.columns() {
            match col {
                Value::Scalar(s) => {
                    columns.push(Value::Scalar(s.clone()));
                }
                Value::Column(c) => columns.push(Value::Column(c.slice(range.clone()))),
            }
        }
        Self {
            columns,
            num_rows: range.end - range.start + 1,
        }
    }
}
