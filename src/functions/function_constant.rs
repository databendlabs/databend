// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataArrayRef, DataSchema, DataType, DataValue};
use crate::error::FuseQueryResult;

use crate::functions::Function;

#[derive(Clone, Debug)]
pub struct ConstantFunction {
    value: DataValue,
}

impl ConstantFunction {
    pub fn create(value: DataValue) -> FuseQueryResult<Function> {
        Ok(Function::Constant(ConstantFunction { value }))
    }

    pub fn name(&self) -> &'static str {
        "ConstantFunction"
    }

    pub fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        self.value.data_type()
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(self.value.is_null())
    }

    pub fn evaluate(&self, _block: &DataBlock) -> FuseQueryResult<DataArrayRef> {
        self.value.to_array()
    }
}

impl fmt::Display for ConstantFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}
