// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{data_array_add, DataColumnarValue, DataSchema, DataType};
use crate::error::FuseQueryResult;
use crate::functions::Function;

#[derive(Clone, Debug)]
pub struct AddFunction {
    left: Box<Function>,
    right: Box<Function>,
}

impl AddFunction {
    pub fn create(args: &[Function]) -> FuseQueryResult<Function> {
        Ok(Function::Add(AddFunction {
            left: Box::from(args[0].clone()),
            right: Box::from(args[1].clone()),
        }))
    }

    pub fn name(&self) -> &'static str {
        "AddFunction"
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        self.left.return_type(input_schema)
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    pub fn evaluate(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Array(data_array_add(
            &self.left.evaluate(block)?,
            &self.right.evaluate(block)?,
        )?))
    }
}

impl fmt::Display for AddFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} + {:?}", self.left, self.right)
    }
}
