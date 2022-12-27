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
use crate::ColumnBuilder;
use crate::ColumnIndex;
use crate::Domain;
use crate::Value;

/// Chunk is a lightweight container for a group of columns.
#[derive(Clone)]
pub struct Chunk<Index: ColumnIndex = usize> {
    columns: Vec<ChunkEntry<Index>>,
    num_rows: usize,
    meta: Option<ChunkMetaInfoPtr>,
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
        Self {
            columns,
            num_rows,
            meta: None,
        }
    }

    #[inline]
    pub fn new_with_meta(
        columns: Vec<ChunkEntry<Index>>,
        num_rows: usize,
        meta: Option<ChunkMetaInfoPtr>,
    ) -> Self {
        debug_assert!(columns.iter().all(|col| match &col.value {
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
    pub fn columns(&self) -> impl Iterator<Item = &ChunkEntry<Index>> {
        self.columns.iter()
    }

    #[inline]
    pub fn columns_ref(&self) -> &[ChunkEntry<Index>] {
        &self.columns
    }

    #[inline]
    pub fn get_by_offset(&self, offset: usize) -> &ChunkEntry<Index> {
        &self.columns[offset]
    }

    #[inline]
    pub fn get_by_id(&self, id: &Index) -> Option<&ChunkEntry<Index>> {
        self.columns().find(|entry| entry.id == *id)
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
        self.columns().map(|entry| entry.memory_size()).sum()
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
                .all(|(col, field)| { &col.data_type == field.data_type() })
        );

        // Return chunk directly, because we don't support decimal yet.
        self.clone()
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
            num_rows: range.end - range.start,
            meta: self.meta.clone(),
        }
    }

    #[inline]
    pub fn add_column(&mut self, column: ChunkEntry<Index>) {
        #[cfg(debug_assertions)]
        if let Value::Column(col) = &column.value {
            assert_eq!(self.num_rows, col.len());
        }
        self.columns.push(column);
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

    /// Resort the columns according to the schema.
    #[inline]
    pub fn resort(self, src_schema: &DataSchema, dest_schema: &DataSchema) -> Result<Self> {
        let columns = dest_schema
            .fields()
            .iter()
            .map(|dest_field| {
                let src_offset = src_schema.index_of(dest_field.name()).map_err(|_| {
                    let valid_fields: Vec<String> = src_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().to_string())
                        .collect();
                    ErrorCode::BadArguments(format!(
                        "Unable to get field named \"{}\". Valid fields: {:?}",
                        dest_field.name(),
                        valid_fields
                    ))
                })?;
                Ok(self.get_by_offset(src_offset).clone())
            })
            .collect::<Result<Vec<_>>>()?;

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
}

// impl Chunk<String> {
//     pub fn from_arrow_chunk<A: AsRef<dyn Array>>(
//         arrow_chunk: &ArrowChunk<A>,
//         schema: &TableSchemaRef,
//     ) -> Result<Self> {
//         let cols = schema
//             .fields
//             .iter()
//             .zip(arrow_chunk.arrays())
//             .map(|(field, col)| ChunkEntry {
//                 id: field.name().clone(),
//                 data_type: field.data_type().into(),
//                 value: Value::Column(Column::from_arrow(col.as_ref())),
//             })
//             .collect();

//         Ok(Chunk::new(cols, arrow_chunk.len()))
//     }

//     /// Resort the columns according to the schema.
//     #[inline]
//     pub fn resort(self, schema: &TableSchema) -> Result<Self> {
//         let mut columns = Vec::with_capacity(self.columns.len());
//         for f in schema.fields() {
//             let mut flag = false;
//             for col in &self.columns {
//                 if col.id.eq(f.name()) {
//                     flag = true;
//                     columns.push(col.clone());
//                 }
//             }
//             if !flag {
//                 let valid_fields: Vec<String> = self.columns.iter().map(|c| c.id.clone()).collect();
//                 return Err(ErrorCode::BadArguments(format!(
//                     "Unable to get field named \"{}\". Valid fields: {:?}",
//                     f.name(),
//                     valid_fields
//                 )));
//             }
//         }

//         Ok(Self {
//             columns,
//             num_rows: self.num_rows,
//             meta: self.meta,
//         })
//     }
// }

impl Chunk<usize> {
    #[inline]
    pub fn new_from_sequence(columns: Vec<(Value<AnyType>, DataType)>, num_rows: usize) -> Self {
        debug_assert!(columns.iter().all(|entry| match &entry.0 {
            Value::Scalar(_) => true,
            Value::Column(c) => c.len() == num_rows,
        }));
        let columns = columns
            .iter()
            .enumerate()
            .map(|(idx, c)| ChunkEntry {
                id: idx,
                data_type: c.1.clone(),
                value: c.0.clone(),
            })
            .collect();

        Self {
            columns,
            num_rows,
            meta: None,
        }
    }

    pub fn from_arrow_chunk<A: AsRef<dyn Array>>(
        arrow_chunk: &ArrowChunk<A>,
        schema: &DataSchema,
    ) -> Result<Self> {
        let cols = schema
            .fields
            .iter()
            .zip(arrow_chunk.arrays())
            .enumerate()
            .map(|(i, (field, col))| {
                Ok(ChunkEntry {
                    id: i,
                    data_type: field.data_type().clone(),
                    value: Value::Column(Column::from_arrow(col.as_ref())),
                })
            })
            .collect::<Result<_>>()?;

        Ok(Chunk::new(cols, arrow_chunk.len()))
    }
}

impl TryFrom<Chunk> for ArrowChunk<ArrayRef> {
    type Error = ErrorCode;

    fn try_from(v: Chunk) -> Result<ArrowChunk<ArrayRef>> {
        let arrays = v
            .convert_to_full()
            .columns()
            .map(|val| {
                let column = val.value.clone().into_column().unwrap();
                column.as_arrow()
            })
            .collect();

        Ok(ArrowChunk::try_new(arrays)?)
    }
}

impl<Index: ColumnIndex> ChunkEntry<Index> {
    pub fn memory_size(&self) -> usize {
        match &self.value {
            Value::Scalar(s) => std::mem::size_of_val(&s),
            Value::Column(c) => c.memory_size(),
        }
    }
}
