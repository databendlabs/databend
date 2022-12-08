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

use std::collections::HashMap;
use std::ops::Range;

use crate::types::AnyType;
use crate::types::DataType;
use crate::ColumnBuilder;
use crate::ColumnIndex;
use crate::Domain;
use crate::Value;

/// Chunk is a lightweight container for a group of columns.
#[derive(Clone)]
pub struct Chunk<Index: ColumnIndex = usize> {
    columns: Vec<ChunkEntry<Index>>,
    num_rows: usize,
}

#[derive(Clone)]
pub struct ChunkEntry<Index: ColumnIndex = usize> {
    pub id: Index,
    pub data_type: DataType,
    pub value: Value<AnyType>,
}

impl<Index: ColumnIndex> Chunk<Index> {
    #[inline]
    pub fn new(columns: Vec<ChunkEntry<Index>>, num_rows: usize) -> Self {
        debug_assert!(columns.iter().all(|entry| match &entry.value {
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
    pub fn columns(&self) -> impl Iterator<Item = &ChunkEntry<Index>> {
        self.columns.iter()
    }

    #[inline]
    pub fn get_by_offset(&self, offset: usize) -> &ChunkEntry<Index> {
        &self.columns[offset]
    }

    #[inline]
    pub fn get_by_id(&self, id: &Index) -> &ChunkEntry<Index> {
        self.columns()
            .find(|entry| entry.id == *id)
            .ok_or_else(|| format!("Chunk doesn't contain a column with id `{id}`"))
            .unwrap()
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
    pub fn domains(&self) -> HashMap<Index, Domain> {
        self.columns
            .iter()
            .map(|entry| (entry.id.clone(), entry.value.as_ref().domain()))
            .collect()
    }

    #[inline]
    pub fn memory_size(&self) -> usize {
        self.columns()
            .map(|entry| match &entry.value {
                Value::Scalar(s) => std::mem::size_of_val(&s),
                Value::Column(c) => c.memory_size(),
            })
            .sum()
    }

    pub fn convert_to_full(&self) -> Self {
        let columns = self
            .columns()
            .map(|entry| match &entry.value {
                Value::Scalar(s) => {
                    let builder =
                        ColumnBuilder::repeat(&s.as_ref(), self.num_rows, &entry.data_type);
                    let col = builder.build();
                    ChunkEntry {
                        id: entry.id.clone(),
                        data_type: entry.data_type.clone(),
                        value: Value::Column(col),
                    }
                }
                Value::Column(c) => ChunkEntry {
                    id: entry.id.clone(),
                    data_type: entry.data_type.clone(),
                    value: Value::Column(c.clone()),
                },
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
            .map(|entry| match &entry.value {
                Value::Scalar(s) => ChunkEntry {
                    id: entry.id.clone(),
                    data_type: entry.data_type.clone(),
                    value: Value::Scalar(s.clone()),
                },
                Value::Column(c) => ChunkEntry {
                    id: entry.id.clone(),
                    data_type: entry.data_type.clone(),
                    value: Value::Column(c.slice(range.clone())),
                },
            })
            .collect();
        Self {
            columns,
            num_rows: range.end - range.start + 1,
        }
    }
}
