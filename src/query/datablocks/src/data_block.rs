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

use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::ArrayRef;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pretty_format_blocks;
use crate::BlockMetaInfoPtr;

#[derive(Clone, Eq, PartialEq)]
pub struct DataBlock {
    schema: DataSchemaRef,
    columns: Vec<ColumnRef>,
    meta: Option<BlockMetaInfoPtr>,
}

impl DataBlock {
    #[inline]
    pub fn create(schema: DataSchemaRef, columns: Vec<ColumnRef>) -> Self {
        debug_assert!(
            schema.fields().iter().zip(columns.iter()).all(|(f, c)| f
                .data_type()
                .data_type_id()
                .to_physical_type()
                == c.data_type().data_type_id().to_physical_type()),
            "Schema: {schema:?}, column types: {:?}",
            &columns
                .iter()
                .map(|c| c.data_type())
                .collect::<Vec<DataTypeImpl>>()
        );
        DataBlock {
            schema,
            columns,
            meta: None,
        }
    }

    #[inline]
    pub fn create_with_meta(
        schema: DataSchemaRef,
        columns: Vec<ColumnRef>,
        meta: Option<BlockMetaInfoPtr>,
    ) -> Self {
        debug_assert!(
            schema.fields().iter().zip(columns.iter()).all(|(f, c)| f
                .data_type()
                .data_type_id()
                .to_physical_type()
                == c.data_type().data_type_id().to_physical_type()),
            "Schema: {schema:?}, column types: {:?}",
            &columns
                .iter()
                .map(|c| c.data_type())
                .collect::<Vec<DataTypeImpl>>()
        );
        DataBlock {
            schema,
            columns,
            meta,
        }
    }

    #[inline]
    pub fn empty() -> Self {
        DataBlock {
            schema: Arc::new(DataSchema::empty()),
            columns: vec![],
            meta: None,
        }
    }

    #[inline]
    pub fn empty_with_schema(schema: DataSchemaRef) -> Self {
        let mut columns = vec![];
        for f in schema.fields().iter() {
            let col = f.data_type().create_column(&[]).unwrap();
            columns.push(col)
        }
        Self::create(schema, columns)
    }

    #[inline]
    pub fn empty_with_meta(meta: BlockMetaInfoPtr) -> Self {
        DataBlock {
            schema: Arc::new(DataSchema::empty()),
            columns: vec![],
            meta: Some(meta),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_columns() == 0 || self.num_rows() == 0
    }

    #[inline]
    pub fn schema(&self) -> &DataSchemaRef {
        &self.schema
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].len()
        }
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Data Block physical memory size
    #[inline]
    pub fn memory_size(&self) -> usize {
        self.columns.iter().map(|x| x.memory_size()).sum()
    }

    #[inline]
    pub fn column(&self, index: usize) -> &ColumnRef {
        &self.columns[index]
    }

    #[inline]
    pub fn columns(&self) -> &[ColumnRef] {
        &self.columns
    }

    #[inline]
    pub fn try_column_by_name(&self, name: &str) -> Result<&ColumnRef> {
        if name == "*" {
            Ok(&self.columns[0])
        } else {
            let idx = self.schema.index_of(name)?;
            Ok(&self.columns[idx])
        }
    }

    /// Take the first data value of the column.
    #[inline]
    pub fn first(&self, col: &str) -> Result<DataValue> {
        let column = self.try_column_by_name(col)?;
        column.get_checked(0)
    }

    /// Take the last data value of the column.
    #[inline]
    pub fn last(&self, col: &str) -> Result<DataValue> {
        let column = self.try_column_by_name(col)?;
        column.get_checked(column.len() - 1)
    }

    #[inline]
    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let rows = self.num_rows();
        if offset == 0 && length >= rows {
            return self.clone();
        }
        let mut limited_columns = Vec::with_capacity(self.num_columns());
        for i in 0..self.num_columns() {
            limited_columns.push(self.column(i).slice(offset, length));
        }
        DataBlock::create_with_meta(self.schema().clone(), limited_columns, self.meta.clone())
    }

    #[inline]
    pub fn add_column(self, column: ColumnRef, field: DataField) -> Result<Self> {
        let mut columns = self.columns.clone();
        let mut fields = self.schema().fields().clone();

        columns.push(column);
        fields.push(field);

        let new_schema = Arc::new(DataSchema::new(fields));

        Ok(Self {
            columns,
            schema: new_schema,
            meta: self.meta,
        })
    }

    #[inline]
    pub fn remove_column_index(self, idx: usize) -> Result<Self> {
        let mut columns = self.columns.clone();
        let mut fields = self.schema().fields().clone();

        columns.remove(idx);
        fields.remove(idx);
        let new_schema = Arc::new(DataSchema::new(fields));

        Ok(Self {
            columns,
            schema: new_schema,
            meta: self.meta,
        })
    }

    #[inline]
    pub fn add_meta(self, meta: Option<BlockMetaInfoPtr>) -> Result<Self> {
        Ok(Self {
            columns: self.columns.clone(),
            schema: self.schema.clone(),
            meta,
        })
    }

    #[inline]
    pub fn get_meta(&self) -> Option<&BlockMetaInfoPtr> {
        self.meta.as_ref()
    }

    #[inline]
    pub fn take_meta(&mut self) -> Option<BlockMetaInfoPtr> {
        self.meta.take()
    }

    #[inline]
    pub fn remove_column(self, name: &str) -> Result<Self> {
        let idx = self.schema.index_of(name)?;
        self.remove_column_index(idx)
    }

    #[inline]
    pub fn resort(self, schema: DataSchemaRef) -> Result<Self> {
        let mut columns = Vec::with_capacity(self.num_columns());
        for f in schema.fields() {
            let column = self.try_column_by_name(f.name())?;
            columns.push(column.clone());
        }

        Ok(Self {
            columns,
            schema,
            meta: self.meta,
        })
    }

    #[inline]
    pub fn meta(&self) -> Result<Option<BlockMetaInfoPtr>> {
        Ok(self.meta.clone())
    }

    #[inline]
    pub fn convert_full_block(self) -> Result<Self> {
        let mut columns = Vec::with_capacity(self.num_columns());
        let schema = self.schema().clone();
        for f in schema.fields() {
            let column = self.try_column_by_name(f.name())?;
            columns.push(column.convert_full_column());
        }

        Ok(Self {
            columns,
            schema,
            meta: self.meta,
        })
    }

    pub fn from_chunk<A: AsRef<dyn Array>>(
        schema: &DataSchemaRef,
        chuck: &Chunk<A>,
    ) -> Result<DataBlock> {
        let columns = chuck
            .columns()
            .iter()
            .zip(schema.fields().iter())
            .map(|(col, f)| match f.is_nullable() {
                true => col.into_nullable_column(),
                false => col.into_column(),
            })
            .collect();

        Ok(DataBlock::create(schema.clone(), columns))
    }

    // If field_marks[i].is_some(), then DataBlock.column[i] = num_rows * DataField.default_value().
    // Else, DataBlock.column[i] = chuck.columns[i]
    pub fn create_with_schema_from_chunk<A: AsRef<dyn Array>>(
        schema: &DataSchemaRef,
        chuck: &Chunk<A>,
        field_marks: &[Option<()>],
        num_rows: usize,
    ) -> Result<DataBlock> {
        let mut data_block = DataBlock::create(Arc::new(DataSchema::empty()), vec![]);
        let mut chunk_idx: usize = 0;
        let chunk_columns = chuck.columns();
        let schema_fields = schema.fields();

        for (i, mark) in field_marks.iter().enumerate() {
            let field = &schema_fields[i];
            let column = if mark.is_some() {
                let default_value = field.data_type().default_value();
                field
                    .data_type()
                    .create_constant_column(&default_value, num_rows)?
            } else {
                assert!(chunk_idx < chunk_columns.len());
                let chunk_column = &chunk_columns[chunk_idx];
                assert_eq!(chunk_column.as_ref().len(), num_rows);
                chunk_idx += 1;
                if field.is_nullable() {
                    chunk_column.into_nullable_column()
                } else {
                    chunk_column.into_column()
                }
            };
            data_block = data_block.add_column(column, field.clone())?;
        }

        Ok(data_block)
    }

    pub fn create_with_schema_from_data_block(
        schema: &DataSchemaRef,
        data_block: DataBlock,
        data_block_column_ids: HashSet<u32>,
        num_rows: usize,
    ) -> Result<DataBlock> {
        let mut new_data_block = DataBlock::create(Arc::new(DataSchema::empty()), vec![]);
        let mut data_block_columns_idx: usize = 0;
        let data_block_columns = data_block.columns();
        let schema_fields = schema.fields();

        for field in schema_fields {
            let column_id = field.column_id().unwrap();
            if !data_block_column_ids.contains(&column_id) {
                let default_value = field.data_type().default_value();
                let column = field
                    .data_type()
                    .create_constant_column(&default_value, num_rows)?;

                new_data_block = new_data_block.add_column(column, field.clone())?;
            } else {
                let chunk_column = &data_block_columns[data_block_columns_idx];
                data_block_columns_idx += 1;
                new_data_block = new_data_block.add_column(chunk_column.clone(), field.clone())?;
            }
        }

        Ok(new_data_block)
    }

    pub fn get_serializers(&self) -> Result<Vec<TypeSerializerImpl>> {
        let columns_size = self.num_columns();

        let mut serializers = vec![];
        for col_index in 0..columns_size {
            let column = self.column(col_index);
            let field = self.schema().field(col_index);
            let data_type = field.data_type();
            let serializer = data_type.create_serializer(column)?;
            serializers.push(serializer);
        }
        Ok(serializers)
    }
}

impl TryFrom<DataBlock> for Chunk<ArrayRef> {
    type Error = ErrorCode;

    fn try_from(v: DataBlock) -> Result<Chunk<ArrayRef>> {
        let arrays = v
            .columns()
            .iter()
            .zip(v.schema.fields().iter())
            .map(|(c, f)| c.as_arrow_array(f.data_type().clone()))
            .collect::<Vec<_>>();

        Ok(Chunk::try_new(arrays)?)
    }
}

impl fmt::Debug for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted = pretty_format_blocks(&[self.clone()]).expect("Pretty format batches error");
        let lines: Vec<&str> = formatted.trim().lines().collect();
        write!(f, "\n{:#?}\n", lines)
    }
}

impl Default for DataBlock {
    fn default() -> Self {
        Self::empty()
    }
}
