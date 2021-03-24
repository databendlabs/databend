// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use fuse_query_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};

use crate::datablocks::DataBlock;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::IFunction;

#[derive(Clone, Debug)]
pub struct ColumnFunction {
    value: String,
    saved: Option<DataColumnarValue>,
}

impl ColumnFunction {
    pub fn try_create(value: &str) -> FuseQueryResult<Box<dyn IFunction>> {
        Ok(Box::new(ColumnFunction {
            value: value.to_string(),
            saved: None,
        }))
    }
}

impl IFunction for ColumnFunction {
    fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        let field = if self.value == "*" {
            input_schema.field(0)
        } else {
            input_schema.field_with_name(self.value.as_str())?
        };

        Ok(field.data_type().clone())
    }

    fn nullable(&self, input_schema: &DataSchema) -> FuseQueryResult<bool> {
        let field = if self.value == "*" {
            input_schema.field(0)
        } else {
            input_schema.field_with_name(self.value.as_str())?
        };
        Ok(field.is_nullable())
    }

    fn eval(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Array(
            block.column_by_name(self.value.as_str())?.clone(),
        ))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        self.saved = Some(DataColumnarValue::Array(
            block.column_by_name(self.value.as_str())?.clone(),
        ));
        Ok(())
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported aggregate operation for function field".to_string(),
        ))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported aggregate operation for function field".to_string(),
        ))
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported aggregate operation for function field".to_string(),
        ))
    }
}

impl fmt::Display for ColumnFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.value)
    }
}
