// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use arrow::array::new_empty_array;

use crate::datavalues::{DataArrayRef, DataSchema, DataSchemaRef};
use crate::error::FuseQueryResult;

#[derive(Debug, Clone)]
pub struct DataBlock {
    schema: DataSchemaRef,
    columns: Vec<DataArrayRef>,
}

impl DataBlock {
    pub fn create(schema: DataSchemaRef, columns: Vec<DataArrayRef>) -> Self {
        DataBlock { schema, columns }
    }

    pub fn try_from_arrow_batch(batch: &arrow::record_batch::RecordBatch) -> FuseQueryResult<Self> {
        Ok(DataBlock::create(
            batch.schema(),
            Vec::from(batch.columns()),
        ))
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
            columns.push(new_empty_array(f.data_type()))
        }
        DataBlock { schema, columns }
    }

    pub fn to_arrow_batch(&self) -> FuseQueryResult<arrow::record_batch::RecordBatch> {
        Ok(arrow::record_batch::RecordBatch::try_new(
            self.schema.clone(),
            self.columns.clone(),
        )?)
    }

    pub fn is_empty(&self) -> bool {
        self.num_columns() == 0 || self.num_rows() == 0
    }

    pub fn schema(&self) -> &DataSchemaRef {
        &self.schema
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].data().len()
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn column(&self, index: usize) -> &DataArrayRef {
        &self.columns[index]
    }

    pub fn column_by_name(&self, name: &str) -> FuseQueryResult<&DataArrayRef> {
        if name == "*" {
            Ok(&self.columns[0])
        } else {
            let idx = self.schema.index_of(name)?;
            Ok(&self.columns[idx])
        }
    }
}
