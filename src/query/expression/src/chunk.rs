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

use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk as ArrowChunk;
use common_arrow::ArrayRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::schema::DataSchema;
use crate::types::AnyType;
use crate::types::DataType;
use crate::ChunkMetaInfoPtr;
use crate::Column;
use crate::DataSchemaRef;
use crate::Domain;
use crate::Scalar;
use crate::Value;

/// Chunk is a lightweight container for a group of columns.
#[derive(Clone)]
pub struct Chunk {
    columns: Vec<(Value<AnyType>, DataType)>,
    num_rows: usize,
    meta: Option<ChunkMetaInfoPtr>,
}

impl Chunk {
    #[inline]
    pub fn new(columns: Vec<(Value<AnyType>, DataType)>, num_rows: usize) -> Self {
        debug_assert!(columns.iter().all(|(col, _)| match col {
            Value::Scalar(_) => true,
            Value::Column(c) => c.len() == num_rows,
        }));
        Self {
            columns,
            num_rows,
            meta: None,
        }
    }

    #[inline]
    pub fn new_with_meta(
        columns: Vec<(Value<AnyType>, DataType)>,
        num_rows: usize,
        meta: Option<ChunkMetaInfoPtr>,
    ) -> Self {
        debug_assert!(columns.iter().all(|(col, _)| match col {
            Value::Scalar(_) => true,
            Value::Column(c) => c.len() == num_rows,
        }));
        Self {
            columns,
            num_rows,
            meta,
        }
    }

    #[inline]
    pub fn empty() -> Self {
        Chunk::new(vec![], 0)
    }

    #[inline]
    pub fn empty_with_meta(meta: ChunkMetaInfoPtr) -> Self {
        Chunk::new_with_meta(vec![], 0, Some(meta))
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
    pub fn domains(&self) -> HashMap<usize, Domain> {
        self.columns
            .iter()
            .map(|(value, _)| value.as_ref().domain())
            .enumerate()
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
            .map(|(col, ty)| {
                let col = col.convert_to_full_column(ty, self.num_rows());
                (Value::Column(col), ty.clone())
            })
            .collect();
        Self {
            columns,
            num_rows: self.num_rows,
            meta: self.meta.clone(),
        }
    }

    /// Convert the columns to fit the type required by schema. This is used to
    /// restore the lost information (e.g. the scale of decimal) before persisting
    /// the columns to storage.
    pub fn fit_schema(&self, schema: DataSchema) -> Self {
        debug_assert!(self.num_columns() == schema.fields().len());
        debug_assert!(
            self.columns
                .iter()
                .zip(schema.fields())
                .all(|((_, ty), field)| { ty == &field.data_type().into() })
        );

        // Return chunk directly, because we don't support decimal yet.
        self.clone()
    }

    /// Take the first Scalar of the column.
    #[inline]
    pub fn first(&self, index: usize) -> Result<Scalar> {
        if self.num_rows == 0 {
            return Err(ErrorCode::EmptyData("Chunk is empty"));
        }
        let (value, _) = self.column(index);
        match value {
            Value::Scalar(s) => Ok(s.clone()),
            Value::Column(c) => Ok(unsafe { c.index_unchecked(0).to_owned() }),
        }
    }

    /// Take the last Scalar of the column.
    #[inline]
    pub fn last(&self, index: usize) -> Result<Scalar> {
        if self.num_rows == 0 {
            return Err(ErrorCode::EmptyData("Chunk is empty"));
        }
        let (value, _) = self.column(index);
        match value {
            Value::Scalar(s) => Ok(s.clone()),
            Value::Column(c) => Ok(unsafe { c.index_unchecked(self.num_rows - 1).to_owned() }),
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
            num_rows: range.end - range.start,
            meta: self.meta.clone(),
        }
    }

    #[inline]
    pub fn add_column(&mut self, column: Value<AnyType>, data_type: DataType) {
        #[cfg(debug_assertions)]
        if let Value::Column(col) = &column {
            assert_eq!(self.num_rows, col.len());
        }
        self.columns.push((column, data_type))
    }

    #[inline]
    pub fn remove_column_index(self, idx: usize) -> Result<Self> {
        let mut columns = self.columns.clone();

        columns.remove(idx);

        Ok(Self {
            columns,
            num_rows: self.num_rows,
            meta: self.meta,
        })
    }

    #[inline]
    pub fn add_meta(self, meta: Option<ChunkMetaInfoPtr>) -> Result<Self> {
        Ok(Self {
            columns: self.columns.clone(),
            num_rows: self.num_rows,
            meta,
        })
    }

    #[inline]
    pub fn get_meta(&self) -> Option<&ChunkMetaInfoPtr> {
        self.meta.as_ref()
    }

    #[inline]
    pub fn meta(&self) -> Result<Option<ChunkMetaInfoPtr>> {
        Ok(self.meta.clone())
    }

    pub fn from_arrow_chunk<A: AsRef<dyn Array>>(
        arrow_chunk: &ArrowChunk<A>,
        schema: &DataSchemaRef,
    ) -> Result<Self> {
        let mut num_rows = 0;
        let columns: Vec<(Value<AnyType>, DataType)> = arrow_chunk
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(c, f)| {
                let col = Column::from_arrow(c.as_ref());
                num_rows = col.len();
                let data_type = f.data_type().into();
                (Value::Column(col), data_type)
            })
            .collect();

        Ok(Self::new(columns, num_rows))
    }
}

impl TryFrom<Chunk> for ArrowChunk<ArrayRef> {
    type Error = ErrorCode;

    fn try_from(v: Chunk) -> Result<ArrowChunk<ArrayRef>> {
        let arrays = v
            .convert_to_full()
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
