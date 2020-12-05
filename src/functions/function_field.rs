// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::Function;

#[derive(Clone, Debug)]
pub struct FieldFunction {
    value: String,
    saved: Option<DataColumnarValue>,
}

impl FieldFunction {
    pub fn try_create(value: &str) -> FuseQueryResult<Function> {
        Ok(Function::Variable(FieldFunction {
            value: value.to_string(),
            saved: None,
        }))
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(input_schema
            .field_with_name(&self.value)?
            .data_type()
            .clone())
    }

    pub fn nullable(&self, input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(input_schema.field_with_name(&self.value)?.is_nullable())
    }

    pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        self.saved = Some(DataColumnarValue::Array(
            block.column_by_name(self.value.as_str())?.clone(),
        ));
        Ok(())
    }

    pub fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Ok(())
    }

    pub fn state(&self) -> FuseQueryResult<Vec<DataValue>> {
        Ok(vec![])
    }

    pub fn result(&self) -> FuseQueryResult<DataColumnarValue> {
        self.saved
            .clone()
            .ok_or_else(|| FuseQueryError::Internal("Result cannot be none".to_string()))
    }
}

impl fmt::Display for FieldFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.value)
    }
}
