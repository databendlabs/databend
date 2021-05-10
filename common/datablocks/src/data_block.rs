// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow;
use common_arrow::arrow::record_batch::RecordBatch;
use common_datavalues::DataArrayRef;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::pretty_format_blocks;

#[derive(Clone)]
pub struct DataBlock {
    schema: DataSchemaRef,
    columns: Vec<DataArrayRef>
}

impl DataBlock {
    pub fn create(schema: DataSchemaRef, columns: Vec<DataArrayRef>) -> Self {
        DataBlock { schema, columns }
    }

    pub fn empty() -> Self {
        DataBlock {
            schema: Arc::new(DataSchema::empty()),
            columns: vec![]
        }
    }

    pub fn empty_with_schema(schema: DataSchemaRef) -> Self {
        let mut columns = vec![];
        for f in schema.fields().iter() {
            columns.push(arrow::array::new_empty_array(f.data_type()))
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
            self.columns[0].data().len()
        }
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Data Block physical memory size
    pub fn memory_size(&self) -> usize {
        self.columns.iter().map(|x| x.get_array_memory_size()).sum()
    }

    pub fn column(&self, index: usize) -> &DataArrayRef {
        &self.columns[index]
    }

    pub fn columns(&self) -> &[DataArrayRef] {
        &self.columns
    }

    pub fn try_column_by_name(&self, name: &str) -> Result<&DataArrayRef> {
        if name == "*" {
            Ok(&self.columns[0])
        } else {
            let idx = self.schema.index_of(name).map_err(ErrorCodes::from_arrow)?;
            Ok(&self.columns[idx])
        }
    }

    pub fn column_by_name(&self, name: &str) -> Option<&DataArrayRef> {
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
}

impl TryInto<arrow::record_batch::RecordBatch> for DataBlock {
    type Error = ErrorCodes;

    fn try_into(self) -> Result<RecordBatch> {
        arrow::record_batch::RecordBatch::try_new(self.schema.clone(), self.columns.clone())
            .map_err(ErrorCodes::from_arrow)
    }
}

impl TryInto<DataBlock> for arrow::record_batch::RecordBatch {
    type Error = ErrorCodes;

    fn try_into(self) -> Result<DataBlock> {
        Ok(DataBlock::create(self.schema(), Vec::from(self.columns())))
    }
}

impl fmt::Debug for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted = pretty_format_blocks(&[self.clone()]).expect("Pretty format batches error");
        let lines: Vec<&str> = formatted.trim().lines().collect();
        write!(f, "\n{:#?}\n", lines)
    }
}
