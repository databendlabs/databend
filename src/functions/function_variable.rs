// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataArrayRef, DataSchema, DataType};
use crate::error::Result;
use crate::functions::Function;

#[derive(Clone, Debug)]
pub struct VariableFunction {
    value: String,
}

impl VariableFunction {
    pub fn create(value: &str) -> Result<Function> {
        Ok(Function::Variable(VariableFunction {
            value: value.to_string(),
        }))
    }

    pub fn name(&self) -> &'static str {
        "VariableFunction"
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> Result<DataType> {
        Ok(input_schema
            .field_with_name(&self.value)?
            .data_type()
            .clone())
    }

    pub fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        Ok(input_schema.field_with_name(&self.value)?.is_nullable())
    }

    pub fn evaluate(&self, block: &DataBlock) -> Result<DataArrayRef> {
        Ok(block.column_by_name(self.value.as_str())?.clone())
    }
}

impl fmt::Display for VariableFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}
