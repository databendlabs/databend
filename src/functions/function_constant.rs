// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;

use super::*;

#[derive(Clone, Debug, PartialEq)]
pub struct ConstantFunction {
    value: DataValue,
}

impl ConstantFunction {
    pub fn create(value: DataValue) -> Result<Function> {
        Ok(Function::Constant(ConstantFunction { value }))
    }

    pub fn name(&self) -> String {
        "ConstantFunction".to_string()
    }

    pub fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        self.value.data_type()
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(self.value.is_null())
    }

    pub fn evaluate(&self, _block: &DataBlock) -> Result<DataArrayRef> {
        self.value.to_array()
    }
}

impl fmt::Display for ConstantFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}
