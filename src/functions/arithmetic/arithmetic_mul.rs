// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datatypes::{array_mul, DataArrayRef, DataSchema, DataType};
use crate::error::Result;

use crate::functions::Function;

#[derive(Clone, Debug)]
pub struct MulFunction {
    left: Box<Function>,
    right: Box<Function>,
}

impl MulFunction {
    pub fn create(args: &[Function]) -> Result<Function> {
        Ok(Function::Mul(MulFunction {
            left: Box::from(args[0].clone()),
            right: Box::from(args[1].clone()),
        }))
    }

    pub fn name(&self) -> &'static str {
        "MulFunction"
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> Result<DataType> {
        self.left.return_type(input_schema)
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    pub fn evaluate(&self, block: &DataBlock) -> Result<DataArrayRef> {
        array_mul(self.left.evaluate(block)?, self.right.evaluate(block)?)
    }
}

impl fmt::Display for MulFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} * {:?}", self.left, self.right)
    }
}
