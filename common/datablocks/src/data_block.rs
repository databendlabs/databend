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

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::record_batch::RecordBatch;
use common_datavalues::columns::DataColumn;
use common_datavalues::series::IntoSeries;
use common_datavalues::series::Series;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pretty_format_blocks;

#[derive(Clone)]
pub struct DataBlock {
    schema: DataSchemaRef,
    columns: Vec<DataColumn>,
}

impl DataBlock {
    #[inline]
    pub fn create(schema: DataSchemaRef, columns: Vec<DataColumn>) -> Self {
        DataBlock { schema, columns }
    }

    #[inline]
    pub fn create_by_array(schema: DataSchemaRef, arrays: Vec<Series>) -> Self {
        let columns = arrays.into_iter().map(DataColumn::Array).collect();
        DataBlock { schema, columns }
    }

    #[inline]
    pub fn empty() -> Self {
        DataBlock {
            schema: Arc::new(DataSchema::empty()),
            columns: vec![],
        }
    }

    #[inline]
    pub fn empty_with_schema(schema: DataSchemaRef) -> Self {
        let mut columns = vec![];
        for f in schema.fields().iter() {
            let array = arrow::array::new_empty_array(f.data_type().to_arrow());
            let array: ArrayRef = Arc::from(array);
            columns.push(DataColumn::Array(array.into_series()))
        }
        DataBlock { schema, columns }
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
        self.columns.iter().map(|x| x.get_array_memory_size()).sum()
    }

    #[inline]
    pub fn column(&self, index: usize) -> &DataColumn {
        &self.columns[index]
    }

    #[inline]
    pub fn columns(&self) -> &[DataColumn] {
        &self.columns
    }

    #[inline]
    pub fn try_column_by_name(&self, name: &str) -> Result<&DataColumn> {
        if name == "*" {
            Ok(&self.columns[0])
        } else {
            let idx = self.schema.index_of(name)?;
            Ok(&self.columns[idx])
        }
    }

    #[inline]
    pub fn try_array_by_name(&self, name: &str) -> Result<Series> {
        if name == "*" {
            self.columns[0].to_array()
        } else {
            let idx = self.schema.index_of(name)?;
            self.columns[idx].to_array()
        }
    }

    /// Take the first data value of the column.
    #[inline]
    pub fn first(&self, col: &str) -> Result<DataValue> {
        let column = self.try_column_by_name(col)?;
        column.try_get(0)
    }

    /// Take the last data value of the column.
    #[inline]
    pub fn last(&self, col: &str) -> Result<DataValue> {
        let column = self.try_column_by_name(col)?;
        column.try_get(column.len() - 1)
    }

    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let rows = self.num_rows();
        if offset == 0 && length >= rows {
            return self.clone();
        }
        let mut limited_columns = Vec::with_capacity(self.num_columns());
        for i in 0..self.num_columns() {
            limited_columns.push(self.column(i).slice(offset, length));
        }
        DataBlock::create(self.schema().clone(), limited_columns)
    }

    #[inline]
    pub fn add_column(self, column: DataColumn, field: DataField) -> Result<Self> {
        let mut columns = self.columns.clone();
        let mut fields = self.schema().fields().clone();

        columns.push(column);
        fields.push(field);

        let new_schema = Arc::new(DataSchema::new(fields));

        Ok(Self {
            columns,
            schema: new_schema,
        })
    }

    #[inline]
    pub fn remove_column(self, name: &str) -> Result<Self> {
        let mut columns = self.columns.clone();
        let mut fields = self.schema().fields().clone();

        let idx = self.schema.index_of(name)?;
        columns.remove(idx);
        fields.remove(idx);
        let new_schema = Arc::new(DataSchema::new(fields));

        Ok(Self {
            columns,
            schema: new_schema,
        })
    }

    #[inline]
    pub fn resort(self, schema: DataSchemaRef) -> Result<Self> {
        let mut columns = Vec::with_capacity(self.num_columns());
        for f in schema.fields() {
            let column = self.try_column_by_name(f.name())?;
            columns.push(column.clone());
        }

        Ok(Self { columns, schema })
    }
}

impl TryFrom<DataBlock> for RecordBatch {
    type Error = ErrorCode;

    fn try_from(v: DataBlock) -> Result<RecordBatch> {
        let arrays = v
            .columns()
            .iter()
            .map(|c| c.to_array().map(|series| series.get_array_ref()))
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch::try_new(Arc::new(v.schema.to_arrow()), arrays)?)
    }
}

impl TryFrom<arrow::record_batch::RecordBatch> for DataBlock {
    type Error = ErrorCode;

    fn try_from(v: arrow::record_batch::RecordBatch) -> Result<DataBlock> {
        let schema = Arc::new(v.schema().as_ref().into());
        let series = v
            .columns()
            .iter()
            .map(|array| array.clone().into_series())
            .collect();
        Ok(DataBlock::create_by_array(schema, series))
    }
}

impl fmt::Debug for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted = pretty_format_blocks(&[self.clone()]).expect("Pretty format batches error");
        let lines: Vec<&str> = formatted.trim().lines().collect();
        write!(f, "\n{:#?}\n", lines)
    }
}
