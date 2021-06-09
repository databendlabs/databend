// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow;
use common_arrow::arrow::record_batch::RecordBatch;
use common_datavalues::DataArrayRef;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pretty_format_blocks;

#[derive(Clone)]
pub struct DataBlock {
    schema: DataSchemaRef,
    columns: Vec<DataColumnarValue>,
}

impl DataBlock {
    pub fn create(schema: DataSchemaRef, columns: Vec<DataColumnarValue>) -> Self {
        DataBlock { schema, columns }
    }

    pub fn create_by_array(schema: DataSchemaRef, arrays: Vec<DataArrayRef>) -> Self {
        let columns = arrays
            .iter()
            .map(|array| DataColumnarValue::Array(array.clone()))
            .collect();
        DataBlock { schema, columns }
    }

    pub fn empty() -> Self {
        DataBlock {
            schema: Arc::new(DataSchema::empty()),
            columns: vec![],
        }
    }

    pub fn empty_with_schema(schema: DataSchemaRef) -> Self {
        let mut columns = vec![];
        for f in schema.fields().iter() {
            columns.push(arrow::array::new_empty_array(f.data_type()).into())
        }
        DataBlock { schema, columns }
    }

    pub fn is_empty(&self) -> bool {
        self.num_columns() == 0 || self.num_rows() == 0
    }

    pub fn schema(&self) -> &DataSchemaRef {
        &self.schema
    }

    pub fn num_rows(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].len()
        }
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Data Block physical memory size
    pub fn memory_size(&self) -> usize {
        self.columns.iter().map(|x| x.get_array_memory_size()).sum()
    }

    pub fn column(&self, index: usize) -> &DataColumnarValue {
        &self.columns[index]
    }

    pub fn columns(&self) -> &[DataColumnarValue] {
        &self.columns
    }

    pub fn try_column_by_name(&self, name: &str) -> Result<&DataColumnarValue> {
        if name == "*" {
            Ok(&self.columns[0])
        } else {
            let idx = self.schema.index_of(name)?;
            Ok(&self.columns[idx])
        }
    }

    pub fn column_by_name(&self, name: &str) -> Option<&DataColumnarValue> {
        if self.is_empty() {
            return None;
        }

        if name == "*" {
            return Some(&self.columns[0]);
        };

        if let Ok(idx) = self.schema.index_of(name) {
            Some(&self.columns[idx])
        } else {
            None
        }
    }

    pub fn try_array_by_name(&self, name: &str) -> Result<DataArrayRef> {
        if name == "*" {
            self.columns[0].to_array()
        } else {
            let idx = self.schema.index_of(name)?;
            self.columns[idx].to_array()
        }
    }
}

impl TryFrom<DataBlock> for RecordBatch {
    type Error = ErrorCode;

    fn try_from(v: DataBlock) -> Result<RecordBatch> {
        let columns = v
            .columns()
            .iter()
            .map(|c| c.to_array())
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(v.schema.clone(), columns)?)
    }
}

impl TryFrom<arrow::record_batch::RecordBatch> for DataBlock {
    type Error = ErrorCode;

    fn try_from(v: arrow::record_batch::RecordBatch) -> Result<DataBlock> {
        Ok(DataBlock::create_by_array(
            v.schema(),
            Vec::from(v.columns()),
        ))
    }
}

impl fmt::Debug for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted = pretty_format_blocks(&[self.clone()]).expect("Pretty format batches error");
        let lines: Vec<&str> = formatted.trim().lines().collect();
        write!(f, "\n{:#?}\n", lines)
    }
}
