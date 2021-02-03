// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::IFunction;

#[derive(Clone, Debug)]
pub struct FieldFunction {
    value: String,
    saved: Option<DataColumnarValue>,
}

impl FieldFunction {
    pub fn try_create(value: &str) -> FuseQueryResult<Box<dyn IFunction>> {
        Ok(Box::new(FieldFunction {
            value: value.to_string(),
            saved: None,
        }))
    }
}

impl IFunction for FieldFunction {
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
        Err(FuseQueryError::Internal(
            "Unsupported aggregate operation for function field".to_string(),
        ))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(
            "Unsupported aggregate operation for function field".to_string(),
        ))
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Err(FuseQueryError::Internal(
            "Unsupported aggregate operation for function field".to_string(),
        ))
    }
}

impl fmt::Display for FieldFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.value)
    }
}
